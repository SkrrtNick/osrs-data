package osrs.cache

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RequirementExtractorTest {

    @Test
    fun `skillName maps ordinals correctly`() {
        assertEquals("attack", RequirementExtractor.skillName(0))
        assertEquals("defence", RequirementExtractor.skillName(1))
        assertEquals("strength", RequirementExtractor.skillName(2))
        assertEquals("hitpoints", RequirementExtractor.skillName(3))
        assertEquals("ranged", RequirementExtractor.skillName(4))
        assertEquals("prayer", RequirementExtractor.skillName(5))
        assertEquals("magic", RequirementExtractor.skillName(6))
    }

    @Test
    fun `extractRequirements finds primary requirement`() {
        val params = mapOf<Int, Any>(434 to 0, 436 to 70) // Attack 70
        val item = ItemDecoder.RawItem(4151, "Abyssal whip", "Wield", params)
        val reqs = RequirementExtractor.extract(item)
        assertEquals(mapOf("attack" to 70), reqs)
    }

    @Test
    fun `extractRequirements finds dual requirements`() {
        val params = mapOf<Int, Any>(
            434 to 4, 436 to 40, // Ranged 40
            435 to 1, 437 to 40  // Defence 40
        )
        val item = ItemDecoder.RawItem(1135, "Green d'hide body", "Wear", params)
        val reqs = RequirementExtractor.extract(item)
        assertEquals(mapOf("ranged" to 40, "defence" to 40), reqs)
    }

    @Test
    fun `extractRequirements skips non-equipable items`() {
        val params = mapOf<Int, Any>(434 to 9, 436 to 10) // Fletching 10 (skilling)
        val item = ItemDecoder.RawItem(50, "Shortbow", "Use", params)
        val reqs = RequirementExtractor.extract(item)
        assertTrue(reqs.isEmpty())
    }

    @Test
    fun `extractRequirements returns empty for no params`() {
        val item = ItemDecoder.RawItem(1, "Bronze sword", "Wield", null)
        val reqs = RequirementExtractor.extract(item)
        assertTrue(reqs.isEmpty())
    }
}
