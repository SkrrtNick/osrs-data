package osrs.cache

/**
 * Read-only buffer for parsing big-endian binary data from the OSRS cache.
 */
class CacheBuffer(private val data: ByteArray, private var pos: Int = 0) {

    val position: Int get() = pos
    val remaining: Int get() = data.size - pos

    fun readByte(): Int {
        return data[pos++].toInt()
    }

    fun readUByte(): Int {
        return data[pos++].toInt() and 0xFF
    }

    fun readShort(): Int {
        val v = (data[pos].toInt() and 0xFF shl 8) or (data[pos + 1].toInt() and 0xFF)
        pos += 2
        return if (v > 0x7FFF) v - 0x10000 else v
    }

    fun readUShort(): Int {
        val v = (data[pos].toInt() and 0xFF shl 8) or (data[pos + 1].toInt() and 0xFF)
        pos += 2
        return v
    }

    fun readUInt24(): Int {
        val v = (data[pos].toInt() and 0xFF shl 16) or
                (data[pos + 1].toInt() and 0xFF shl 8) or
                (data[pos + 2].toInt() and 0xFF)
        pos += 3
        return v
    }

    fun readInt(): Int {
        val v = (data[pos].toInt() and 0xFF shl 24) or
                (data[pos + 1].toInt() and 0xFF shl 16) or
                (data[pos + 2].toInt() and 0xFF shl 8) or
                (data[pos + 3].toInt() and 0xFF)
        pos += 4
        return v
    }

    fun readLong(): Long {
        val hi = readInt().toLong() and 0xFFFFFFFFL
        val lo = readInt().toLong() and 0xFFFFFFFFL
        return (hi shl 32) or lo
    }

    fun readString(): String {
        val start = pos
        while (data[pos].toInt() != 0) pos++
        val s = String(data, start, pos - start, Charsets.ISO_8859_1)
        pos++ // skip null terminator
        return s
    }

    fun readBigSmart(): Int {
        return if (data[pos].toInt() and 0xFF >= 128) {
            readInt() and 0x7FFFFFFF
        } else {
            readUShort()
        }
    }

    fun readBytes(length: Int): ByteArray {
        val result = data.copyOfRange(pos, pos + length)
        pos += length
        return result
    }

    fun slice(offset: Int, length: Int): CacheBuffer {
        return CacheBuffer(data.copyOfRange(offset, offset + length))
    }

    fun skip(n: Int) {
        pos += n
    }
}
