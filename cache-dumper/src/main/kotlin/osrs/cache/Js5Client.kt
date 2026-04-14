package osrs.cache

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Client for the Jagex JS5 protocol.
 *
 * Connects to an OSRS world server on port 43594, performs the JS5
 * handshake, and downloads raw archive groups. No authentication
 * is required -- the JS5 service is public.
 */
class Js5Client private constructor(
    private val socket: Socket,
    private val input: DataInputStream,
) : AutoCloseable {

    /**
     * Request and download a single group from the server.
     *
     * @param archive Archive index (e.g., 255 for meta, 2 for configs)
     * @param group Group index within the archive
     * @return Raw container bytes (compression header + payload)
     */
    fun download(archive: Int, group: Int): ByteArray {
        // Send urgent request
        socket.getOutputStream().write(buildRequest(urgent = true, archive = archive, group = group))
        socket.getOutputStream().flush()

        // Read response header to determine total size
        val headerBuf = ByteArray(8)
        input.readFully(headerBuf)

        val respArchive = headerBuf[0].toInt() and 0xFF
        val respGroup = ((headerBuf[1].toInt() and 0xFF) shl 8) or (headerBuf[2].toInt() and 0xFF)
        require(respArchive == archive && respGroup == group) {
            "Response mismatch: expected $archive/$group, got $respArchive/$respGroup"
        }

        val compressionType = headerBuf[3].toInt() and 0xFF
        val compressedSize = ((headerBuf[4].toInt() and 0xFF) shl 24) or
                ((headerBuf[5].toInt() and 0xFF) shl 16) or
                ((headerBuf[6].toInt() and 0xFF) shl 8) or
                (headerBuf[7].toInt() and 0xFF)

        // Total container size: compression(1) + compressedSize(4) + payload
        // If compressed, there's also a 4-byte decompressed size
        val containerSize = if (compressionType == 0) {
            compressedSize + 5
        } else {
            compressedSize + 9
        }

        // Read remaining data, accounting for 512-byte block boundaries
        // First block: 8 header bytes already read, remaining = min(504, containerSize - 5)
        val out = ByteArrayOutputStream(containerSize)
        out.write(headerBuf, 3, 5) // compression type + compressed size

        var bytesRead = 5 // we've captured 5 bytes of container data
        var blockBytesRead = 8 // position within current 512-byte block

        while (bytesRead < containerSize) {
            if (blockBytesRead == 512) {
                // Read and verify continuation marker
                val marker = input.read()
                require(marker == 0xFF) { "Expected continuation marker 0xFF, got $marker" }
                blockBytesRead = 1
            }

            val blockRemaining = 512 - blockBytesRead
            val dataRemaining = containerSize - bytesRead
            val toRead = minOf(blockRemaining, dataRemaining)

            val chunk = ByteArray(toRead)
            input.readFully(chunk)
            out.write(chunk)

            bytesRead += toRead
            blockBytesRead += toRead
        }

        return out.toByteArray()
    }

    override fun close() {
        socket.close()
    }

    companion object {
        private const val JS5_PORT = 43594
        private const val CONFIG_URL = "https://oldschool1.runescape.com/jav_config.ws"

        /**
         * Fetch the current cache revision from Jagex's config endpoint.
         */
        fun fetchRevision(): Int {
            val client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build()
            val request = HttpRequest.newBuilder()
                .uri(URI.create(CONFIG_URL))
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            require(response.statusCode() == 200) { "Failed to fetch jav_config: ${response.statusCode()}" }
            return parseRevision(response.body())
        }

        fun parseRevision(config: String): Int {
            for (line in config.lines()) {
                if (line.startsWith("param=25=")) {
                    return line.substringAfterLast("=").trim().toInt()
                }
            }
            error("Could not find param=25 (cache revision) in jav_config")
        }

        fun buildHandshake(revision: Int): ByteArray {
            val buf = ByteArray(21)
            buf[0] = 15 // JS5 service
            buf[1] = (revision shr 24).toByte()
            buf[2] = (revision shr 16).toByte()
            buf[3] = (revision shr 8).toByte()
            buf[4] = revision.toByte()
            // bytes 5-20 = XTEA key (all zeros)
            return buf
        }

        fun buildRequest(urgent: Boolean, archive: Int, group: Int): ByteArray {
            return byteArrayOf(
                if (urgent) 1 else 0,
                archive.toByte(),
                (group shr 8).toByte(),
                group.toByte()
            )
        }

        /**
         * Assemble raw response bytes (with block markers) into clean container data.
         * Used for testing -- the main download() method does this streaming.
         */
        fun assembleResponse(raw: ByteArray, archive: Int, group: Int): ByteArray {
            val out = ByteArrayOutputStream()

            // Container header starts at byte 3 (after archive(1) + group(2))
            val compressionType = raw[3].toInt() and 0xFF
            val compressedSize = ((raw[4].toInt() and 0xFF) shl 24) or
                    ((raw[5].toInt() and 0xFF) shl 16) or
                    ((raw[6].toInt() and 0xFF) shl 8) or
                    (raw[7].toInt() and 0xFF)

            val containerSize = if (compressionType == 0) compressedSize + 5 else compressedSize + 9

            var pos = 3 // start after archive/group header
            var blockPos = 3
            var written = 0

            while (written < containerSize && pos < raw.size) {
                if (blockPos == 512) {
                    pos++ // skip 0xFF marker
                    blockPos = 1
                }
                out.write(raw[pos].toInt())
                pos++
                blockPos++
                written++
            }

            return out.toByteArray()
        }

        /**
         * Connect to Jagex and perform the JS5 handshake.
         *
         * @param host World server hostname
         * @param revision Cache revision from jav_config
         * @return Connected Js5Client ready for downloads
         */
        fun connect(
            host: String = "oldschool1.runescape.com",
            revision: Int = fetchRevision()
        ): Js5Client {
            val socket = Socket(host, JS5_PORT)
            socket.soTimeout = 30_000

            // Send handshake
            socket.getOutputStream().write(buildHandshake(revision))
            socket.getOutputStream().flush()

            // Read status
            val status = socket.getInputStream().read()
            require(status == 0) { "JS5 handshake failed with status $status (revision=$revision)" }

            return Js5Client(socket, DataInputStream(socket.getInputStream()))
        }
    }
}
