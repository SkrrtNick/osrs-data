package osrs.cache

import kotlin.test.Test
import kotlin.test.assertEquals

class CacheBufferTest {

    @Test
    fun `readUByte reads unsigned byte`() {
        val buf = CacheBuffer(byteArrayOf(0xFF.toByte(), 0x00))
        assertEquals(255, buf.readUByte())
        assertEquals(0, buf.readUByte())
    }

    @Test
    fun `readUShort reads big-endian unsigned short`() {
        val buf = CacheBuffer(byteArrayOf(0x01, 0x00))
        assertEquals(256, buf.readUShort())
    }

    @Test
    fun `readInt reads big-endian signed int`() {
        val buf = CacheBuffer(byteArrayOf(0x00, 0x00, 0x00, 0x0A))
        assertEquals(10, buf.readInt())
    }

    @Test
    fun `readUInt24 reads 3-byte big-endian unsigned int`() {
        val buf = CacheBuffer(byteArrayOf(0x01, 0x00, 0x00))
        assertEquals(65536, buf.readUInt24())
    }

    @Test
    fun `readString reads null-terminated string`() {
        val buf = CacheBuffer(byteArrayOf(0x48, 0x69, 0x00, 0x01))
        assertEquals("Hi", buf.readString())
        assertEquals(3, buf.position)
    }

    @Test
    fun `readBigSmart reads short when high bit clear`() {
        val buf = CacheBuffer(byteArrayOf(0x00, 0x05))
        assertEquals(5, buf.readBigSmart())
    }

    @Test
    fun `readBigSmart reads int when high bit set`() {
        // 0x80000005 with high bit set -> value = 0x00000005
        val buf = CacheBuffer(byteArrayOf(0x80.toByte(), 0x00, 0x00, 0x05))
        assertEquals(5, buf.readBigSmart())
    }

    @Test
    fun `readLong reads big-endian signed long`() {
        val buf = CacheBuffer(byteArrayOf(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07))
        assertEquals(7L, buf.readLong())
    }

    @Test
    fun `slice returns sub-buffer`() {
        val buf = CacheBuffer(byteArrayOf(0x01, 0x02, 0x03, 0x04, 0x05))
        val sub = buf.slice(1, 3)
        assertEquals(2, sub.readUByte())
        assertEquals(3, sub.readUByte())
        assertEquals(4, sub.readUByte())
    }
}
