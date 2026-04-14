package osrs.cache

/**
 * Parses archive index metadata and unpacks multi-file groups.
 */
object IndexData {

    /**
     * Parse the index for an archive. Returns group metadata needed
     * to determine file counts and IDs within each group.
     *
     * @return Map of groupId -> GroupInfo(fileCount, fileIds)
     */
    // Flag bits for the index header (per OpenRS2 Js5Index)
    private const val FLAG_NAMES = 0x01
    private const val FLAG_DIGESTS = 0x02
    private const val FLAG_LENGTHS = 0x04
    private const val FLAG_UNCOMPRESSED_CHECKSUMS = 0x08

    fun parse(data: ByteArray): Map<Int, GroupInfo> {
        val buf = CacheBuffer(data)
        val protocol = buf.readUByte()
        require(protocol in 5..7) { "Unsupported index protocol: $protocol" }

        if (protocol >= 6) buf.readInt() // revision

        val flags = buf.readUByte()
        val smart = protocol >= 7

        val groupCount = if (smart) buf.readBigSmart() else buf.readUShort()

        // 1. Read delta-encoded group IDs
        val groupIds = IntArray(groupCount)
        var accumId = 0
        for (i in 0 until groupCount) {
            val delta = if (smart) buf.readBigSmart() else buf.readUShort()
            accumId += delta
            groupIds[i] = accumId
        }

        // 2. Skip name hashes if present (FLAG_NAMES = 0x01)
        if (flags and FLAG_NAMES != 0) buf.skip(groupCount * 4)

        // 3. Skip CRC32 checksums
        buf.skip(groupCount * 4)

        // 4. Skip uncompressed checksums if present (FLAG_UNCOMPRESSED_CHECKSUMS = 0x08)
        if (flags and FLAG_UNCOMPRESSED_CHECKSUMS != 0) buf.skip(groupCount * 4)

        // 5. Skip whirlpool digests if present (FLAG_DIGESTS = 0x02, 64 bytes each)
        if (flags and FLAG_DIGESTS != 0) buf.skip(groupCount * 64)

        // 6. Skip compressed + decompressed lengths if present (FLAG_LENGTHS = 0x04)
        if (flags and FLAG_LENGTHS != 0) buf.skip(groupCount * 8)

        // 7. Skip version/revision per group
        buf.skip(groupCount * 4)

        // 8. Read file counts per group
        val fileCounts = IntArray(groupCount)
        for (i in 0 until groupCount) {
            fileCounts[i] = if (smart) buf.readBigSmart() else buf.readUShort()
        }

        // 9. Read file IDs per group (delta-encoded)
        val fileIds = Array(groupCount) { IntArray(0) }
        for (i in 0 until groupCount) {
            val ids = IntArray(fileCounts[i])
            var fileAccum = 0
            for (j in 0 until fileCounts[i]) {
                val delta = if (smart) buf.readBigSmart() else buf.readUShort()
                fileAccum += delta
                ids[j] = fileAccum
            }
            fileIds[i] = ids
        }

        // 10. Skip file name hashes if present (FLAG_NAMES = 0x01)
        if (flags and FLAG_NAMES != 0) {
            for (i in 0 until groupCount) {
                buf.skip(fileCounts[i] * 4)
            }
        }

        // Build result map
        val result = mutableMapOf<Int, GroupInfo>()
        for (i in 0 until groupCount) {
            result[groupIds[i]] = GroupInfo(fileCounts[i], fileIds[i])
        }
        return result
    }

    /**
     * Unpack a multi-file group into individual file byte arrays.
     *
     * @param data Decompressed group data
     * @param fileCount Number of files in the group
     * @param fileIds The file IDs (used as keys in the returned map)
     * @return Map of fileId -> file data bytes
     */
    fun unpackGroup(data: ByteArray, fileCount: Int, fileIds: IntArray): Map<Int, ByteArray> {
        if (fileCount == 1) {
            return mapOf(fileIds[0] to data)
        }

        // Multi-file group: trailer at end describes chunk sizes
        val chunks = data[data.size - 1].toInt() and 0xFF

        // Read delta-encoded sizes from trailer
        val trailerStart = data.size - 1 - (chunks * fileCount * 4)
        val trailerBuf = CacheBuffer(data.copyOfRange(trailerStart, data.size - 1))

        val fileSizes = IntArray(fileCount)
        for (chunk in 0 until chunks) {
            var delta = 0
            for (file in 0 until fileCount) {
                delta += trailerBuf.readInt()
                fileSizes[file] += delta
            }
        }

        // Allocate file buffers
        val files = Array(fileCount) { ByteArray(fileSizes[it]) }
        val fileOffsets = IntArray(fileCount)

        // Read data chunks
        var dataPos = 0
        for (chunk in 0 until chunks) {
            var chunkDelta = 0
            val sizesBuf = CacheBuffer(data.copyOfRange(
                trailerStart + chunk * fileCount * 4,
                trailerStart + (chunk + 1) * fileCount * 4
            ))
            for (file in 0 until fileCount) {
                chunkDelta += sizesBuf.readInt()
                System.arraycopy(data, dataPos, files[file], fileOffsets[file], chunkDelta)
                fileOffsets[file] += chunkDelta
                dataPos += chunkDelta
            }
        }

        return fileIds.withIndex().associate { (idx, id) -> id to files[idx] }
    }

    data class GroupInfo(val fileCount: Int, val fileIds: IntArray)
}
