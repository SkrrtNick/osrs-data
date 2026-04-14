package osrs.cache

/**
 * Extracts equipment skill requirements from decoded item definitions.
 *
 * Requirements are stored as param pairs in the item's params map:
 * - (434, 436): primary skill type + level
 * - (435, 437): secondary skill type + level
 * - (191, 613), (579, 614), (610, 615), (611, 616), (612, 617): slots 3-7
 *
 * The skill type param holds an integer ordinal mapping to a skill name.
 */
object RequirementExtractor {

    private val SKILL_NAMES = arrayOf(
        "attack", "defence", "strength", "hitpoints", "ranged",
        "prayer", "magic", "cooking", "woodcutting", "fletching",
        "fishing", "firemaking", "crafting", "smithing", "mining",
        "herblore", "agility", "thieving", "slayer", "farming",
        "runecraft", "hunter", "construction", "sailing"
    )

    /** Param ID pairs: (skill type param, skill level param) */
    private val REQUIREMENT_PARAMS = listOf(
        434 to 436,
        435 to 437,
        191 to 613,
        579 to 614,
        610 to 615,
        611 to 616,
        612 to 617
    )

    fun skillName(ordinal: Int): String {
        return if (ordinal in SKILL_NAMES.indices) SKILL_NAMES[ordinal] else "unknown_$ordinal"
    }

    /**
     * Extract equipment requirements from a decoded item.
     *
     * @return Map of skill name to required level, empty if not equipable or no requirements
     */
    fun extract(item: ItemDecoder.RawItem): Map<String, Int> {
        if (!item.isEquipable) return emptyMap()
        val params = item.params ?: return emptyMap()

        val requirements = mutableMapOf<String, Int>()
        for ((typeParam, levelParam) in REQUIREMENT_PARAMS) {
            val skillOrdinal = params[typeParam]
            val level = params[levelParam]
            if (skillOrdinal is Int && level is Int && level > 0) {
                requirements[skillName(skillOrdinal)] = level
            }
        }
        return requirements
    }

    /**
     * Process all decoded items and return a map of itemId -> requirements.
     * Only includes items that have at least one requirement.
     */
    fun extractAll(items: Map<Int, ItemDecoder.RawItem>): Map<Int, Map<String, Int>> {
        val result = mutableMapOf<Int, Map<String, Int>>()
        for ((id, item) in items) {
            val reqs = extract(item)
            if (reqs.isNotEmpty()) {
                result[id] = reqs
            }
        }
        return result
    }
}
