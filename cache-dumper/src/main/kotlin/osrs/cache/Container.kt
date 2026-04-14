package osrs.cache

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream

/**
 * Decompresses OSRS cache containers.
 *
 * Container format: [compressionType:1][compressedSize:4][decompressedSize:4?][payload]
 * Compression types: 0=none, 1=bzip2, 2=gzip
 */
object Container {

    fun decompress(data: ByteArray): ByteArray {
        val buf = CacheBuffer(data)
        val compression = buf.readUByte()
        val compressedSize = buf.readInt()

        return when (compression) {
            0 -> {
                // No compression -- payload starts at offset 5
                buf.readBytes(compressedSize)
            }
            1 -> {
                // BZIP2 -- prepend "BZh1" header that the cache strips
                val decompressedSize = buf.readInt()
                val compressed = buf.readBytes(compressedSize)
                decompressBzip2(compressed, decompressedSize)
            }
            2 -> {
                // GZIP
                val decompressedSize = buf.readInt()
                val compressed = buf.readBytes(compressedSize)
                decompressGzip(compressed, decompressedSize)
            }
            else -> error("Unknown compression type: $compression")
        }
    }

    private fun decompressGzip(compressed: ByteArray, expectedSize: Int): ByteArray {
        return GZIPInputStream(ByteArrayInputStream(compressed)).use { gis ->
            val out = ByteArrayOutputStream(expectedSize)
            gis.copyTo(out)
            out.toByteArray()
        }
    }

    private fun decompressBzip2(compressed: ByteArray, expectedSize: Int): ByteArray {
        // BZIP2 in the cache has the "BZh1" (4-byte) magic header stripped.
        // We need to prepend it before decompressing.
        val withHeader = ByteArray(4 + compressed.size)
        withHeader[0] = 'B'.code.toByte()
        withHeader[1] = 'Z'.code.toByte()
        withHeader[2] = 'h'.code.toByte()
        withHeader[3] = '1'.code.toByte()
        compressed.copyInto(withHeader, 4)

        // Use Apache Commons Compress if available at runtime
        try {
            val cls = Class.forName("org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream")
            val constructor = cls.getConstructor(java.io.InputStream::class.java)
            val stream = constructor.newInstance(ByteArrayInputStream(withHeader)) as java.io.InputStream
            return stream.use {
                val out = ByteArrayOutputStream(expectedSize)
                it.copyTo(out)
                out.toByteArray()
            }
        } catch (_: ClassNotFoundException) {
            // If no bzip2 library available, this compression type won't work.
            // Item definitions are typically gzip -- bzip2 is rare for config groups.
            error("BZIP2 decompression requires org.apache.commons:commons-compress on classpath")
        }
    }
}
