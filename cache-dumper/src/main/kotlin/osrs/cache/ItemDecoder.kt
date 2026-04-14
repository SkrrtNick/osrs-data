package osrs.cache

/**
 * Decodes binary OSRS item definitions.
 *
 * Each item is a stream of [opcode][data] pairs terminated by opcode 0.
 * We only extract the fields needed for equipment requirement detection:
 * name (opcode 2), inventoryOption2 (opcode 36), and params (opcode 249).
 */
object ItemDecoder {

    data class RawItem(
        val id: Int,
        val name: String,
        val inventoryOption2: String?,
        val params: Map<Int, Any>?
    ) {
        val isEquipable: Boolean
            get() = inventoryOption2 in setOf("Wield", "Wear", "Equip")
    }

    fun decode(id: Int, data: ByteArray): RawItem {
        val buf = CacheBuffer(data)
        var name = ""
        var iop2: String? = null
        var params: MutableMap<Int, Any>? = null

        while (true) {
            val opcode = buf.readUByte()
            if (opcode == 0) break

            when (opcode) {
                1 -> buf.readUShort() // inventoryModel (short)
                2 -> name = buf.readString()
                3 -> buf.readString() // examine
                4 -> buf.readUShort() // zoom2d
                5 -> buf.readUShort() // xan2d
                6 -> buf.readUShort() // yan2d
                7 -> buf.readUShort() // xOffset2d
                8 -> buf.readUShort() // yOffset2d
                9 -> buf.readString() // unknown1
                11 -> {} // stackable
                12 -> buf.readInt() // cost
                13 -> buf.readUByte() // wearPos1
                14 -> buf.readUByte() // wearPos2
                16 -> {} // members
                23 -> { buf.readUShort(); buf.readUByte() } // maleModel0 (short) + offset
                24 -> buf.readUShort() // maleModel1 (short)
                25 -> { buf.readUShort(); buf.readUByte() } // femaleModel0 (short) + offset
                26 -> buf.readUShort() // femaleModel1 (short)
                27 -> buf.readUByte() // wearPos3
                in 30..34 -> buf.readString() // groundOptions (delegated to entityOpsLoader.decodeOp)
                35 -> buf.readString() // interfaceOptions[0]
                36 -> iop2 = buf.readString() // interfaceOptions[1] -- THE KEY ONE
                37 -> buf.readString() // interfaceOptions[2]
                38 -> buf.readString() // interfaceOptions[3]
                39 -> buf.readString() // interfaceOptions[4]
                40 -> { val n = buf.readUByte(); repeat(n) { buf.readUShort(); buf.readUShort() } } // recolor
                41 -> { val n = buf.readUByte(); repeat(n) { buf.readUShort(); buf.readUShort() } } // retexture
                42 -> buf.readByte() // shiftClickDropIndex (signed byte)
                43 -> {
                    // subops: ubyte opId, then loop: ubyte subopId (0-terminated), string
                    buf.readUByte() // opId
                    while (true) {
                        val subopId = buf.readUByte()
                        if (subopId == 0) break
                        buf.readString()
                    }
                }
                44 -> buf.readInt() // inventoryModel (int, newer version of opcode 1)
                45 -> { buf.readInt(); buf.readUByte() } // maleModel0 (int) + offset
                46 -> buf.readInt() // maleModel1 (int)
                47 -> buf.readInt() // maleModel2 (int)
                48 -> { buf.readInt(); buf.readUByte() } // femaleModel0 (int) + offset
                49 -> buf.readInt() // femaleModel1 (int)
                50 -> buf.readInt() // femaleModel2 (int)
                51 -> buf.readInt() // maleHeadModel (int)
                52 -> buf.readInt() // maleHeadModel2 (int)
                53 -> buf.readInt() // femaleHeadModel (int)
                54 -> buf.readInt() // femaleHeadModel2 (int)
                65 -> {} // tradeable
                75 -> buf.readShort() // weight
                78 -> buf.readUShort() // maleModel2 (short, legacy)
                79 -> buf.readUShort() // femaleModel2 (short, legacy)
                90 -> buf.readUShort() // maleHeadModel (short, legacy)
                91 -> buf.readUShort() // femaleHeadModel (short, legacy)
                92 -> buf.readUShort() // maleHeadModel2 (short, legacy)
                93 -> buf.readUShort() // femaleHeadModel2 (short, legacy)
                94 -> buf.readUShort() // category
                95 -> buf.readUShort() // zan2d
                97 -> buf.readUShort() // notedID
                98 -> buf.readUShort() // notedTemplate
                in 100..109 -> { buf.readUShort(); buf.readUShort() } // count stack overrides
                110 -> buf.readUShort() // resizeX
                111 -> buf.readUShort() // resizeY
                112 -> buf.readUShort() // resizeZ
                113 -> buf.readByte() // ambient
                114 -> buf.readByte() // contrast
                115 -> buf.readUByte() // team
                139 -> buf.readUShort() // boughtId
                140 -> buf.readUShort() // boughtTemplate
                148 -> buf.readUShort() // placeholderId
                149 -> buf.readUShort() // placeholderTemplate
                200 -> {
                    // entityOpsLoader.decodeSubOp: ubyte index + ubyte subId + string
                    buf.readUByte(); buf.readUByte(); buf.readString()
                }
                201 -> {
                    // entityOpsLoader.decodeConditionalOp: ubyte + ushort + ushort + int + int + string
                    buf.readUByte(); buf.readUShort(); buf.readUShort(); buf.readInt(); buf.readInt(); buf.readString()
                }
                202 -> {
                    // entityOpsLoader.decodeConditionalSubOp: ubyte + ushort + ushort + ushort + int + int + string
                    buf.readUByte(); buf.readUShort(); buf.readUShort(); buf.readUShort(); buf.readInt(); buf.readInt(); buf.readString()
                }
                249 -> {
                    // Params map
                    val count = buf.readUByte()
                    params = mutableMapOf()
                    repeat(count) {
                        val typeFlag = buf.readUByte()
                        val paramId = buf.readUInt24()
                        val value: Any = when (typeFlag) {
                            1 -> buf.readString()
                            else -> buf.readInt()
                        }
                        params[paramId] = value
                    }
                }
                else -> {
                    // Unknown opcode -- we can't safely skip it without knowing its size.
                    // This shouldn't happen for known cache revisions.
                    error("Unknown item opcode $opcode at position ${buf.position} for item $id")
                }
            }
        }

        return RawItem(id, name, iop2, params)
    }
}
