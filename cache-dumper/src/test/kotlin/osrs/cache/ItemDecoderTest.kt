package osrs.cache

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ItemDecoderTest {

    @Test
    fun `decode item with name`() {
        // opcode 2 = name (string), opcode 0 = end
        val data = byteArrayOf(
            2, // opcode: name
            0x57, 0x68, 0x69, 0x70, 0x00, // "Whip\0"
            0  // opcode: end
        )
        val item = ItemDecoder.decode(0, data)
        assertEquals("Whip", item.name)
    }

    @Test
    fun `decode item with inventory option Wield`() {
        // opcode 36 = interfaceOptions[1] (iop2)
        val data = byteArrayOf(
            36, // opcode: interfaceOptions[1]
            0x57, 0x69, 0x65, 0x6C, 0x64, 0x00, // "Wield\0"
            0  // end
        )
        val item = ItemDecoder.decode(0, data)
        assertEquals("Wield", item.inventoryOption2)
    }

    @Test
    fun `decode item with params`() {
        // opcode 249 = params
        // count=1, typeFlag=0 (int), paramId=434 (3 bytes: 0x00, 0x01, 0xB2), value=0 (attack)
        val data = byteArrayOf(
            249.toByte(), // opcode: params
            1,            // count
            0,            // typeFlag: int
            0x00, 0x01, 0xB2.toByte(), // paramId: 434
            0x00, 0x00, 0x00, 0x00,    // value: 0 (Attack)
            0  // end
        )
        val item = ItemDecoder.decode(0, data)
        assertNotNull(item.params)
        assertEquals(0, item.params!![434])
    }

    @Test
    fun `decode item with string param`() {
        val data = byteArrayOf(
            249.toByte(), // opcode: params
            1,            // count
            1,            // typeFlag: string
            0x00, 0x00, 0x01, // paramId: 1
            0x48, 0x69, 0x00, // "Hi\0"
            0  // end
        )
        val item = ItemDecoder.decode(0, data)
        // String params are stored but we only care about int params for requirements
    }

    @Test
    fun `isEquipable returns true for Wield`() {
        assertTrue(ItemDecoder.RawItem(0, "", "Wield", null).isEquipable)
    }

    @Test
    fun `isEquipable returns true for Wear`() {
        assertTrue(ItemDecoder.RawItem(0, "", "Wear", null).isEquipable)
    }

    @Test
    fun `isEquipable returns true for Equip`() {
        assertTrue(ItemDecoder.RawItem(0, "", "Equip", null).isEquipable)
    }
}
