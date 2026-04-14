package osrs.cache

import com.google.gson.GsonBuilder
import java.io.File

fun main(args: Array<String>) {
    val outputPath = args.firstOrNull() ?: "data/requirements.json"

    println("=== OSRS Cache Requirement Extractor ===")

    // Step 1: Get current cache revision
    print("Fetching cache revision... ")
    val revision = Js5Client.fetchRevision()
    println("rev $revision")

    // Step 2: Connect via JS5
    print("Connecting to Jagex JS5 server... ")
    val client = Js5Client.connect(revision = revision)
    println("connected")

    client.use {
        // Step 3: Download config archive index
        print("Downloading config index... ")
        val indexRaw = client.download(255, 2)
        val indexData = Container.decompress(indexRaw)
        val index = IndexData.parse(indexData)
        println("${index.size} groups")

        // Step 4: Download item definitions (group 10)
        val itemGroup = index[10] ?: error("Item definition group (10) not found in config index")
        print("Downloading item definitions (${itemGroup.fileCount} items)... ")
        val itemRaw = client.download(2, 10)
        val itemData = Container.decompress(itemRaw)
        println("${itemData.size} bytes")

        // Step 5: Unpack individual item files
        print("Unpacking items... ")
        val files = IndexData.unpackGroup(itemData, itemGroup.fileCount, itemGroup.fileIds)
        println("${files.size} items")

        // Step 6: Decode item definitions
        print("Decoding items... ")
        val items = mutableMapOf<Int, ItemDecoder.RawItem>()
        for ((fileId, fileData) in files) {
            try {
                items[fileId] = ItemDecoder.decode(fileId, fileData)
            } catch (e: Exception) {
                System.err.println("Warning: failed to decode item $fileId: ${e.message}")
            }
        }
        println("${items.size} decoded")

        // Step 7: Extract requirements
        print("Extracting requirements... ")
        val requirements = RequirementExtractor.extractAll(items)
        println("${requirements.size} items with requirements")

        // Step 8: Write JSON
        val output = File(outputPath)
        output.parentFile?.mkdirs()
        val gson = GsonBuilder().setPrettyPrinting().create()
        val json = requirements.entries
            .sortedBy { it.key }
            .associate { (id, reqs) -> id.toString() to reqs }
        output.writeText(gson.toJson(json))
        println("Wrote ${output.absolutePath}")
    }

    println("=== Done ===")
}
