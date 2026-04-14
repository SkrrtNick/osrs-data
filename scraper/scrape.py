#!/usr/bin/env python3
"""
OSRS Wiki Bucket API scraper.

Pulls data from the OSRS Wiki and outputs structured JSON files
that match the Kotlin data classes in Community-Api Models.kt.

Exit codes:
  0 = success
  1 = fatal error (API failure, parse error, empty bucket)
  2 = drift warning (entry count changed >10% from last scrape)
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://oldschool.runescape.wiki/api.php"
USER_AGENT = "osrs-data-scraper (https://github.com/SkrrtNick/osrs-data)"
PAGE_SIZE = 5000
REQUEST_DELAY = 0.5  # seconds between paginated requests
DRIFT_THRESHOLD = 0.10  # 10 %
SCHEMA_SAMPLE_SIZE = 20  # entries to validate per bucket
DATA_VERSION = 1

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def fetch_bucket(bucket_name: str, fields: list[str]) -> list[dict]:
    """
    Fetch all rows from a Wiki Bucket API endpoint, paginating automatically.
    Aborts the process on any HTTP or JSON error.
    """
    all_results: list[dict] = []
    offset = 0
    field_list = ",".join(f"'{f}'" for f in fields)

    while True:
        query = (
            f"bucket('{bucket_name}')"
            f".select({field_list})"
            f".limit({PAGE_SIZE})"
            f".offset({offset})"
            f".run()"
        )
        params = {
            "action": "bucket",
            "format": "json",
            "query": query,
        }

        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
        except requests.RequestException as exc:
            print(f"FATAL: HTTP error fetching {bucket_name} offset={offset}: {exc}", file=sys.stderr)
            sys.exit(1)

        if resp.status_code != 200:
            print(
                f"FATAL: Non-200 status {resp.status_code} for {bucket_name} offset={offset}",
                file=sys.stderr,
            )
            sys.exit(1)

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            print(f"FATAL: Unparseable JSON for {bucket_name} offset={offset}: {exc}", file=sys.stderr)
            sys.exit(1)

        # Check for API-level errors
        if "error" in data:
            print(
                f"FATAL: Bucket API error for {bucket_name}: {data['error']}",
                file=sys.stderr,
            )
            sys.exit(1)

        # The Bucket API returns results under the "bucket" key
        results = data.get("bucket", [])
        all_results.extend(results)

        if len(results) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    print(f"  {bucket_name}: {len(all_results)} entries")
    return all_results


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def safe_int(val: Any, default: int | None = None) -> int | None:
    """Parse a value to int, returning *default* on failure."""
    if val is None or val == "" or val == "N/A":
        return default
    # Handle array values (some bucket fields return arrays)
    if isinstance(val, list):
        if len(val) > 0:
            return safe_int(val[0], default)
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        # Handle floats stored as strings like "4.0"
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return default


def safe_float(val: Any, default: float | None = None) -> float | None:
    if val is None or val == "" or val == "N/A":
        return default
    if isinstance(val, list):
        if len(val) > 0:
            return safe_float(val[0], default)
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_bool(val: Any, default: bool = False) -> bool:
    if val is None or val == "":
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("yes", "true", "1")
    return bool(val)


def safe_str(val: Any, default: str | None = None) -> str | None:
    if val is None or val == "":
        return default
    if isinstance(val, list):
        if len(val) > 0:
            return str(val[0])
        return default
    return str(val)


def safe_str_list(val: Any) -> list[str]:
    """Extract a list of strings from a value (may be string or list)."""
    if val is None or val == "":
        return []
    if isinstance(val, list):
        return [str(v) for v in val if v is not None and v != ""]
    return [str(val)]


def parse_immune(val: Any) -> bool | None:
    """Parse immune fields: 'Immune' -> True, 'Not immune' -> False, else None."""
    if val is None or val == "":
        return None
    s = str(val).strip().lower()
    if s == "immune":
        return True
    if s == "not immune":
        return False
    # Fallback
    return safe_bool(val)


# ---------------------------------------------------------------------------
# Transformers — one per output file
# ---------------------------------------------------------------------------


def load_requirements() -> dict[str, dict[str, int]]:
    """Load equipment requirements from cache-dumper output."""
    req_path = DATA_DIR / "requirements.json"
    if not req_path.exists():
        print("  WARNING: requirements.json not found, equipment requirements will be empty")
        return {}
    try:
        data = json.loads(req_path.read_text(encoding="utf-8"))
        print(f"  Loaded {len(data)} equipment requirements from cache")
        return data
    except Exception as e:
        print(f"  WARNING: failed to load requirements.json: {e}")
        return {}


def build_items(raw_items: list[dict], raw_bonuses: list[dict]) -> dict:
    """
    Merge infobox_item + infobox_bonuses into ItemDefinition keyed by id string.
    Bonuses are matched by page_name_sub (item name).
    """
    requirements = load_requirements()

    # Index bonuses by page_name_sub for merging
    bonuses_by_name: dict[str, dict] = {}
    for b in raw_bonuses:
        name = b.get("page_name_sub", "")
        if name:
            bonuses_by_name[name] = b

    items: dict[str, dict] = {}
    for row in raw_items:
        # item_id is an array of strings
        item_ids = row.get("item_id", [])
        if isinstance(item_ids, list):
            ids = [safe_int(x) for x in item_ids if safe_int(x) is not None]
        else:
            ids = [safe_int(item_ids)]
            ids = [x for x in ids if x is not None]

        if not ids:
            continue

        name = safe_str(row.get("item_name"), "")
        bonus = bonuses_by_name.get(name, {})

        # Determine if tradeable on GE (has buy_limit implies GE tradeable)
        buy_limit = safe_int(row.get("buy_limit"))
        tradeable_raw = safe_str(row.get("tradeable"), "")
        is_tradeable = tradeable_raw.lower() in ("yes", "true", "1") if tradeable_raw else buy_limit is not None

        equipment = None
        slot = safe_str(bonus.get("equipment_slot"))
        if slot:
            equipment = {
                "slot": slot,
                "attackStab": safe_int(bonus.get("stab_attack_bonus"), 0),
                "attackSlash": safe_int(bonus.get("slash_attack_bonus"), 0),
                "attackCrush": safe_int(bonus.get("crush_attack_bonus"), 0),
                "attackMagic": safe_int(bonus.get("magic_attack_bonus"), 0),
                "attackRanged": safe_int(bonus.get("range_attack_bonus"), 0),
                "defenceStab": safe_int(bonus.get("stab_defence_bonus"), 0),
                "defenceSlash": safe_int(bonus.get("slash_defence_bonus"), 0),
                "defenceCrush": safe_int(bonus.get("crush_defence_bonus"), 0),
                "defenceMagic": safe_int(bonus.get("magic_defence_bonus"), 0),
                "defenceRanged": safe_int(bonus.get("range_defence_bonus"), 0),
                "meleeStrength": safe_int(bonus.get("strength_bonus"), 0),
                "rangedStrength": safe_int(bonus.get("ranged_strength_bonus"), 0),
                "magicDamage": safe_float(bonus.get("magic_damage_bonus"), 0.0),
                "prayer": safe_int(bonus.get("prayer_bonus"), 0),
                "requirements": {},  # Populated below from cache-dumper output
            }

        weapon = None
        aspeed = safe_int(bonus.get("weapon_attack_speed"))
        if aspeed is not None:
            weapon = {
                "attackSpeed": aspeed,
                "weaponType": safe_str(bonus.get("combat_style"), ""),
                "stances": [],  # Stances not available from bucket API
            }

        # Compute high/low alch from value if not directly available
        value = safe_int(row.get("value"), 0)
        high_alch = safe_int(row.get("high_alchemy_value"))
        # low_alchemy_value is not in the bucket; compute from value
        low_alch = None
        if high_alch is not None:
            low_alch = int(high_alch * 2 / 3) if high_alch > 0 else 0

        # Create one entry per ID (some items have multiple IDs)
        for item_id in ids:
            # Merge cache-based equipment requirements if available
            item_equipment = equipment
            if item_equipment is not None:
                reqs = requirements.get(str(item_id), {})
                if reqs:
                    item_equipment = {**item_equipment, "requirements": reqs}

            item = {
                "id": item_id,
                "name": name,
                "members": safe_bool(row.get("is_members_only")),
                "tradeable": is_tradeable,
                "tradeableOnGe": buy_limit is not None,
                "stackable": False,  # Not directly available in bucket
                "cost": value,
                "highAlch": high_alch,
                "lowAlch": low_alch,
                "buyLimit": buy_limit,
                "weight": safe_float(row.get("weight")),
                "examine": safe_str(row.get("examine")),
                "questItem": safe_bool(row.get("quest")),
                "equipment": item_equipment,
                "weapon": weapon,
            }
            items[str(item_id)] = item

    return items


def build_monsters(raw: list[dict]) -> dict:
    """MonsterDefinition keyed by id string."""
    monsters: dict[str, dict] = {}
    for row in raw:
        # id can be an array of strings
        monster_ids = row.get("id", [])
        if isinstance(monster_ids, list):
            ids = [safe_int(x) for x in monster_ids if safe_int(x) is not None]
        else:
            ids = [safe_int(monster_ids)]
            ids = [x for x in ids if x is not None]

        if not ids:
            continue

        name = safe_str(row.get("name"), "")

        # max_hit can be an array
        max_hit_raw = row.get("max_hit")
        if isinstance(max_hit_raw, list):
            max_hit = safe_str(max_hit_raw[0] if max_hit_raw else None)
        else:
            max_hit = safe_str(max_hit_raw)

        # slayer_category and assigned_by can be arrays
        slayer_cats = safe_str_list(row.get("slayer_category"))
        slayer_category = slayer_cats[0] if slayer_cats else None
        assigned_list = safe_str_list(row.get("assigned_by"))
        assigned_by = ",".join(assigned_list) if assigned_list else None

        for monster_id in ids:
            monster = {
                "id": monster_id,
                "name": name,
                "members": safe_bool(row.get("is_members_only")),
                "combatLevel": safe_int(row.get("combat_level")),
                "hitpoints": safe_int(row.get("hitpoints")),
                "maxHit": max_hit,
                "attackSpeed": safe_int(row.get("attack_speed")),
                "size": safe_int(row.get("size")),
                "attackLevel": safe_int(row.get("attack_level")),
                "strengthLevel": safe_int(row.get("strength_level")),
                "defenceLevel": safe_int(row.get("defence_level")),
                "magicLevel": safe_int(row.get("magic_level")),
                "rangedLevel": safe_int(row.get("ranged_level")),
                "attackStab": safe_int(row.get("stab_attack_bonus")),
                "attackSlash": safe_int(row.get("slash_attack_bonus")),
                "attackCrush": safe_int(row.get("crush_attack_bonus")),
                "attackMagic": safe_int(row.get("magic_attack_bonus")),
                "attackRanged": safe_int(row.get("range_attack_bonus")),
                "defenceStab": safe_int(row.get("stab_defence_bonus")),
                "defenceSlash": safe_int(row.get("slash_defence_bonus")),
                "defenceCrush": safe_int(row.get("crush_defence_bonus")),
                "defenceMagic": safe_int(row.get("magic_defence_bonus")),
                "defenceRanged": safe_int(row.get("range_defence_bonus")),
                "strengthBonus": safe_int(row.get("strength_bonus")),
                "rangedStrengthBonus": safe_int(row.get("range_strength_bonus")),
                "magicDamageBonus": safe_int(row.get("magic_damage_bonus")),
                "slayerLevel": safe_int(row.get("slayer_level")),
                "slayerXp": safe_float(row.get("slayer_experience")),
                "slayerCategory": slayer_category,
                "assignedBy": assigned_by,
                "elementalWeakness": safe_str(row.get("elemental_weakness")),
                "elementalWeaknessPercent": safe_int(row.get("elemental_weakness_percent")),
                "poisonous": safe_str(row.get("poisonous")),
                "immunePoison": parse_immune(row.get("poison_immune")),
                "immuneVenom": parse_immune(row.get("venom_immune")),
                "examine": safe_str(row.get("examine")),
            }
            monsters[str(monster_id)] = monster

    return monsters


def build_drops(raw: list[dict]) -> list[dict]:
    """DropEntry as a flat JSON array, parsed from dropsline bucket."""
    drops: list[dict] = []
    for row in raw:
        # The page_name is the monster name (source page)
        monster_name = safe_str(row.get("page_name"), "")
        item_name = safe_str(row.get("item_name"), "")

        # Parse the drop_json field for detailed drop info
        drop_json_raw = safe_str(row.get("drop_json"), "")
        drop_data = {}
        if drop_json_raw:
            try:
                drop_data = json.loads(drop_json_raw)
            except (json.JSONDecodeError, ValueError):
                pass

        # Use Dropped from if available (more accurate than page_name)
        if drop_data.get("Dropped from"):
            monster_name = drop_data["Dropped from"]
        # Use Dropped item if available
        if drop_data.get("Dropped item"):
            item_name = drop_data["Dropped item"]

        if not monster_name and not item_name:
            continue

        # Parse rarity from drop_json
        rarity_raw = drop_data.get("Rarity", "0")
        rarity = 0.0
        if rarity_raw:
            rarity_str = str(rarity_raw).strip().lower()
            if rarity_str in ("always", "1", "1/1"):
                rarity = 1.0
            elif rarity_str == "varies":
                rarity = 0.0
            elif "/" in str(rarity_raw):
                parts = str(rarity_raw).split("/")
                try:
                    rarity = float(parts[0].strip()) / float(parts[1].strip())
                except (ValueError, ZeroDivisionError, IndexError):
                    rarity = 0.0
            else:
                rarity = safe_float(rarity_raw, 0.0) or 0.0

        # Parse quantity
        quantity = safe_str(drop_data.get("Drop Quantity"), "1")

        # Rolls
        rolls = safe_int(drop_data.get("Rolls"), 1)

        # Name notes indicate noted drops
        name_notes = safe_str(drop_data.get("Name Notes"), "")
        noted = "noted" in (name_notes or "").lower()

        drop = {
            "monsterName": monster_name,
            "itemName": item_name,
            "itemId": None,  # Not available directly in bucket
            "quantity": quantity,
            "rarity": rarity,
            "noted": noted,
            "rolls": rolls,
        }
        drops.append(drop)

    return drops


def build_quests(raw: list[dict]) -> dict:
    """QuestDefinition keyed by quest name."""
    quests: dict[str, dict] = {}
    for row in raw:
        name = safe_str(row.get("page_name_sub"), safe_str(row.get("page_name"), ""))
        if not name:
            continue

        quest = {
            "name": name,
            "difficulty": safe_str(row.get("official_difficulty")),
            "length": safe_str(row.get("official_length")),
            "requirements": safe_str(row.get("requirements")),
            "startPoint": safe_str(row.get("start_point")),
            "itemsRequired": safe_str(row.get("items_required")),
            "enemiesToDefeat": safe_str(row.get("enemies_to_defeat")),
            "ironmanConcerns": safe_str(row.get("ironman_concerns")),
        }
        quests[name] = quest

    return quests


def build_recipes(raw: list[dict]) -> dict:
    """RecipeDefinition keyed by recipe name, parsed from production_json."""
    recipes: dict[str, dict] = {}
    for row in raw:
        # Parse production_json for the detailed recipe data
        prod_json_raw = safe_str(row.get("production_json"), "")
        prod_data = {}
        if prod_json_raw:
            try:
                prod_data = json.loads(prod_json_raw)
            except (json.JSONDecodeError, ValueError):
                pass

        name = prod_data.get("name", "")
        if not name:
            name = safe_str(row.get("page_name_sub"), safe_str(row.get("page_name"), ""))
        if not name:
            continue

        # Parse materials from production_json
        materials: list[dict] = []
        raw_mats = prod_data.get("materials", [])
        if isinstance(raw_mats, list):
            for mat in raw_mats:
                if isinstance(mat, dict):
                    mat_name = mat.get("name", "")
                    mat_qty = safe_int(mat.get("quantity"), 1)
                    if mat_name:
                        materials.append({"name": mat_name, "quantity": mat_qty})
                elif isinstance(mat, str) and mat:
                    materials.append({"name": mat, "quantity": 1})

        # Parse tools from uses_tool field or production_json
        tools: list[str] = []
        tools_raw = row.get("uses_tool")
        if isinstance(tools_raw, list):
            tools = [str(t) for t in tools_raw if t]
        elif tools_raw:
            tools = [str(tools_raw)]

        # Parse facilities from uses_facility field
        facilities: list[str] = []
        fac_raw = row.get("uses_facility")
        if isinstance(fac_raw, list):
            facilities = [str(f) for f in fac_raw if f]
        elif fac_raw:
            facilities = [str(fac_raw)]

        # Parse skill info from production_json
        skills = prod_data.get("skills", [])
        skill = None
        level = None
        experience = None
        boostable = safe_bool(row.get("is_boostable"))
        if isinstance(skills, list) and skills:
            first_skill = skills[0]
            if isinstance(first_skill, dict):
                skill = first_skill.get("name")
                level = safe_int(first_skill.get("level"))
                experience = safe_float(first_skill.get("experience"))
                if first_skill.get("boostable"):
                    boostable = safe_bool(first_skill.get("boostable"))

        recipe = {
            "name": name,
            "members": safe_bool(row.get("is_members_only")),
            "skill": skill,
            "level": level,
            "experience": experience,
            "materials": materials,
            "tools": tools,
            "facilities": facilities,
            "boostable": boostable,
        }
        recipes[name] = recipe

    return recipes


def build_shops(raw_shops: list[dict], raw_storelines: list[dict]) -> dict:
    """ShopDefinition keyed by shop name. Storelines merged as items."""
    # Group storeline items by sold_by (shop name)
    store_items_by_shop: dict[str, list[dict]] = {}
    for row in raw_storelines:
        shop_name = safe_str(row.get("sold_by"), safe_str(row.get("page_name"), ""))
        if not shop_name:
            continue

        item = {
            "itemName": safe_str(row.get("sold_item"), ""),
            "stock": safe_int(row.get("store_stock")),
            "buyPrice": safe_str(row.get("store_buy_price")),
            "sellPrice": safe_str(row.get("store_sell_price")),
            "currency": safe_str(row.get("store_currency")),
            "restockTime": safe_str(row.get("restock_time")),
        }
        store_items_by_shop.setdefault(shop_name, []).append(item)

    shops: dict[str, dict] = {}
    for row in raw_shops:
        name = safe_str(row.get("page_name_sub"), safe_str(row.get("page_name"), ""))
        if not name:
            continue

        shop = {
            "name": name,
            "owner": safe_str(row.get("owner")),
            "location": safe_str(row.get("location")),
            "members": safe_bool(row.get("is_members_only")),
            "items": store_items_by_shop.get(name, []),
        }
        shops[name] = shop

    return shops


def build_spells(raw: list[dict]) -> dict:
    """SpellDefinition keyed by spell name, parsed from json field."""
    spells: dict[str, dict] = {}
    for row in raw:
        name = safe_str(row.get("page_name_sub"), safe_str(row.get("page_name"), ""))
        if not name:
            continue

        # Parse the json field for level, experience, type
        spell_json_raw = safe_str(row.get("json"), "")
        spell_data = {}
        if spell_json_raw:
            try:
                spell_data = json.loads(spell_json_raw)
            except (json.JSONDecodeError, ValueError):
                pass

        # Parse runes from uses_material (array of rune names)
        rune_names = safe_str_list(row.get("uses_material"))
        # The json field has a "cost" field with HTML rune quantities
        # Parse rune quantities from the cost HTML: <sup>N</sup>[[File:X rune.png|...]]
        runes: dict[str, int] = {}
        cost_html = spell_data.get("cost", "")
        if cost_html and rune_names:
            # Pattern: <sup>N</sup>[[File:Rune.png|Rune|link=Rune name]]
            for m in re.finditer(r"<sup>(\d+)</sup>\[\[File:([^|]+?)\.png", str(cost_html)):
                qty = int(m.group(1))
                rune_file = m.group(2)
                # Match file name to rune name
                runes[rune_file] = qty

            # If regex didn't match, try simpler patterns
            if not runes:
                for rune_name in rune_names:
                    runes[rune_name] = 1

        spellbook = safe_str(row.get("spellbook"), "Standard")
        # Capitalize first letter
        if spellbook:
            spellbook = spellbook[0].upper() + spellbook[1:] if len(spellbook) > 1 else spellbook.upper()
            # Map common bucket values
            spellbook_map = {
                "Normal": "Standard",
                "normal": "Standard",
                "Regular": "Standard",
                "regular": "Standard",
            }
            spellbook = spellbook_map.get(spellbook, spellbook)

        spell = {
            "name": name,
            "spellbook": spellbook,
            "level": safe_int(spell_data.get("level"), 1),
            "experience": safe_float(spell_data.get("exp"), 0.0),
            "type": safe_str(spell_data.get("type"), ""),
            "runes": runes,
            "members": safe_bool(row.get("is_members_only")),
        }
        spells[name] = spell

    return spells


def build_varbits(raw: list[dict]) -> dict:
    """VarbitDefinition keyed by index string."""
    varbits: dict[str, dict] = {}
    for row in raw:
        index = safe_int(row.get("index"))
        if index is None:
            continue

        varbit = {
            "index": index,
            "name": safe_str(row.get("name"), ""),
            "content": safe_str(row.get("content")),
        }
        varbits[str(index)] = varbit

    return varbits


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

# Required fields per output type (field name -> expected type name)
REQUIRED_FIELDS: dict[str, dict[str, str]] = {
    "items": {"id": "int", "name": "str"},
    "monsters": {"id": "int", "name": "str"},
    "drops": {"monsterName": "str", "itemName": "str", "quantity": "str", "rarity": "float"},
    "quests": {"name": "str"},
    "recipes": {"name": "str"},
    "shops": {"name": "str"},
    "spells": {"name": "str", "spellbook": "str", "level": "int"},
    "varbits": {"index": "int", "name": "str"},
}

TYPE_CHECKS = {
    "int": lambda v: isinstance(v, int),
    "float": lambda v: isinstance(v, (int, float)),
    "str": lambda v: isinstance(v, str),
    "bool": lambda v: isinstance(v, bool),
}


def validate_schema(name: str, data: dict | list) -> bool:
    """
    Validate required fields on a sample of entries.
    Returns True if valid, False otherwise.
    """
    required = REQUIRED_FIELDS.get(name, {})
    if not required:
        return True

    # Get a sample of entries
    if isinstance(data, dict):
        entries = list(data.values())[:SCHEMA_SAMPLE_SIZE]
    else:
        entries = data[:SCHEMA_SAMPLE_SIZE]

    for i, entry in enumerate(entries):
        for field, type_name in required.items():
            if field not in entry:
                print(
                    f"FATAL: Schema validation failed for {name}: "
                    f"entry {i} missing required field '{field}'",
                    file=sys.stderr,
                )
                return False
            val = entry[field]
            if val is not None and not TYPE_CHECKS[type_name](val):
                print(
                    f"FATAL: Schema validation failed for {name}: "
                    f"entry {i} field '{field}' expected {type_name}, got {type(val).__name__}",
                    file=sys.stderr,
                )
                return False

    return True


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------


def check_drift(new_counts: dict[str, int]) -> bool:
    """
    Compare entry counts with the previous metadata.json.
    Returns True if drift exceeds threshold.
    """
    metadata_path = DATA_DIR / "metadata.json"
    if not metadata_path.exists():
        return False  # First run, nothing to compare

    try:
        with open(metadata_path) as f:
            old_meta = json.load(f)
    except (json.JSONDecodeError, IOError):
        return False  # Can't read old metadata, skip drift check

    old_files = old_meta.get("files", {})
    drift_detected = False

    for file_name, new_count in new_counts.items():
        old_info = old_files.get(file_name)
        if old_info is None:
            continue
        old_count = old_info.get("entries", 0)
        if old_count == 0:
            continue

        change = abs(new_count - old_count) / old_count
        if change > DRIFT_THRESHOLD:
            print(
                f"DRIFT: {file_name}: {old_count} -> {new_count} "
                f"({change:.1%} change exceeds {DRIFT_THRESHOLD:.0%} threshold)",
                file=sys.stderr,
            )
            drift_detected = True

    return drift_detected


def check_empty_bucket(name: str, data: dict | list, metadata_path: Path) -> None:
    """
    Abort if a bucket returned 0 entries but previously had data.
    """
    count = len(data)
    if count > 0:
        return

    if not metadata_path.exists():
        return  # First run

    try:
        with open(metadata_path) as f:
            old_meta = json.load(f)
    except (json.JSONDecodeError, IOError):
        return

    old_files = old_meta.get("files", {})
    file_name = f"{name}.json"
    old_info = old_files.get(file_name)
    if old_info and old_info.get("entries", 0) > 0:
        print(
            f"FATAL: {name} returned 0 entries but previously had "
            f"{old_info['entries']} entries. Aborting to prevent data loss.",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def write_json(filename: str, data: Any) -> tuple[str, int]:
    """
    Write data as JSON to the data directory.
    Returns (sha256 hex digest, entry count).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / filename
    content = json.dumps(data, indent=2, ensure_ascii=False)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
        f.write("\n")

    hex_digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    sha = f"sha256:{hex_digest}"
    count = len(data)
    return sha, count


def write_metadata(file_info: dict[str, dict]) -> None:
    """Write metadata.json."""
    metadata = {
        "version": DATA_VERSION,
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": file_info,
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / "metadata.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
        f.write("\n")


# ---------------------------------------------------------------------------
# Bucket field selections
# ---------------------------------------------------------------------------

# Fields to request from each bucket — these are the actual wiki bucket column names.
ITEM_FIELDS = [
    "item_id", "item_name", "is_members_only", "tradeable",
    "value", "high_alchemy_value", "weight", "examine", "quest", "buy_limit",
]

BONUSES_FIELDS = [
    "page_name", "page_name_sub", "equipment_slot",
    "stab_attack_bonus", "slash_attack_bonus", "crush_attack_bonus",
    "magic_attack_bonus", "range_attack_bonus",
    "stab_defence_bonus", "slash_defence_bonus", "crush_defence_bonus",
    "magic_defence_bonus", "range_defence_bonus",
    "strength_bonus", "ranged_strength_bonus", "prayer_bonus", "magic_damage_bonus",
    "weapon_attack_speed", "combat_style",
]

MONSTER_FIELDS = [
    "id", "name", "is_members_only", "combat_level", "hitpoints",
    "max_hit", "attack_speed", "size",
    "attack_level", "strength_level", "defence_level", "magic_level", "ranged_level",
    "stab_attack_bonus", "slash_attack_bonus", "crush_attack_bonus",
    "magic_attack_bonus", "range_attack_bonus",
    "stab_defence_bonus", "slash_defence_bonus", "crush_defence_bonus",
    "magic_defence_bonus", "range_defence_bonus",
    "strength_bonus", "range_strength_bonus", "magic_damage_bonus",
    "slayer_level", "slayer_experience", "slayer_category", "assigned_by",
    "elemental_weakness", "elemental_weakness_percent",
    "poisonous", "poison_immune", "venom_immune", "examine",
]

DROP_FIELDS = [
    "page_name", "item_name", "drop_json",
]

QUEST_FIELDS = [
    "page_name", "page_name_sub",
    "official_difficulty", "official_length", "requirements",
    "start_point", "items_required", "enemies_to_defeat", "ironman_concerns",
]

RECIPE_FIELDS = [
    "page_name", "page_name_sub",
    "uses_material", "uses_tool", "uses_facility",
    "is_members_only", "is_boostable", "uses_skill", "production_json",
]

SHOP_FIELDS = [
    "page_name", "page_name_sub", "owner", "location", "is_members_only",
]

STORELINE_FIELDS = [
    "page_name", "page_name_sub", "sold_by", "sold_item",
    "store_buy_price", "store_sell_price", "store_currency",
    "store_stock", "restock_time",
]

SPELL_FIELDS = [
    "page_name", "page_name_sub",
    "is_members_only", "spellbook", "uses_material", "json",
]

VARBIT_FIELDS = [
    "index", "name", "content",
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    metadata_path = DATA_DIR / "metadata.json"
    file_info: dict[str, dict] = {}
    entry_counts: dict[str, int] = {}
    drift = False

    print("=== OSRS Data Scraper ===")
    print()

    # ----- Items (infobox_item + infobox_bonuses) -----
    print("[1/8] Fetching items...")
    raw_items = fetch_bucket("infobox_item", ITEM_FIELDS)
    time.sleep(REQUEST_DELAY)
    raw_bonuses = fetch_bucket("infobox_bonuses", BONUSES_FIELDS)
    time.sleep(REQUEST_DELAY)
    items = build_items(raw_items, raw_bonuses)
    check_empty_bucket("items", items, metadata_path)
    if not validate_schema("items", items):
        return 1
    sha, count = write_json("items.json", items)
    file_info["items.json"] = {"hash": sha, "entries": count}
    entry_counts["items.json"] = count

    # ----- Monsters -----
    print("[2/8] Fetching monsters...")
    raw_monsters = fetch_bucket("infobox_monster", MONSTER_FIELDS)
    time.sleep(REQUEST_DELAY)
    monsters = build_monsters(raw_monsters)
    check_empty_bucket("monsters", monsters, metadata_path)
    if not validate_schema("monsters", monsters):
        return 1
    sha, count = write_json("monsters.json", monsters)
    file_info["monsters.json"] = {"hash": sha, "entries": count}
    entry_counts["monsters.json"] = count

    # ----- Drops -----
    print("[3/8] Fetching drops...")
    raw_drops = fetch_bucket("dropsline", DROP_FIELDS)
    time.sleep(REQUEST_DELAY)
    drops = build_drops(raw_drops)
    check_empty_bucket("drops", drops, metadata_path)
    if not validate_schema("drops", drops):
        return 1
    sha, count = write_json("drops.json", drops)
    file_info["drops.json"] = {"hash": sha, "entries": count}
    entry_counts["drops.json"] = count

    # ----- Quests -----
    print("[4/8] Fetching quests...")
    raw_quests = fetch_bucket("quest", QUEST_FIELDS)
    time.sleep(REQUEST_DELAY)
    quests = build_quests(raw_quests)
    check_empty_bucket("quests", quests, metadata_path)
    if not validate_schema("quests", quests):
        return 1
    sha, count = write_json("quests.json", quests)
    file_info["quests.json"] = {"hash": sha, "entries": count}
    entry_counts["quests.json"] = count

    # ----- Recipes -----
    print("[5/8] Fetching recipes...")
    raw_recipes = fetch_bucket("recipe", RECIPE_FIELDS)
    time.sleep(REQUEST_DELAY)
    recipes = build_recipes(raw_recipes)
    check_empty_bucket("recipes", recipes, metadata_path)
    if not validate_schema("recipes", recipes):
        return 1
    sha, count = write_json("recipes.json", recipes)
    file_info["recipes.json"] = {"hash": sha, "entries": count}
    entry_counts["recipes.json"] = count

    # ----- Shops (infobox_shop + storeline) -----
    print("[6/8] Fetching shops...")
    raw_shops = fetch_bucket("infobox_shop", SHOP_FIELDS)
    time.sleep(REQUEST_DELAY)
    raw_storelines = fetch_bucket("storeline", STORELINE_FIELDS)
    time.sleep(REQUEST_DELAY)
    shops = build_shops(raw_shops, raw_storelines)
    check_empty_bucket("shops", shops, metadata_path)
    if not validate_schema("shops", shops):
        return 1
    sha, count = write_json("shops.json", shops)
    file_info["shops.json"] = {"hash": sha, "entries": count}
    entry_counts["shops.json"] = count

    # ----- Spells -----
    print("[7/8] Fetching spells...")
    raw_spells = fetch_bucket("infobox_spell", SPELL_FIELDS)
    time.sleep(REQUEST_DELAY)
    spells = build_spells(raw_spells)
    check_empty_bucket("spells", spells, metadata_path)
    if not validate_schema("spells", spells):
        return 1
    sha, count = write_json("spells.json", spells)
    file_info["spells.json"] = {"hash": sha, "entries": count}
    entry_counts["spells.json"] = count

    # ----- Varbits -----
    print("[8/8] Fetching varbits...")
    raw_varbits = fetch_bucket("varbit", VARBIT_FIELDS)
    varbits = build_varbits(raw_varbits)
    check_empty_bucket("varbits", varbits, metadata_path)
    if not validate_schema("varbits", varbits):
        return 1
    sha, count = write_json("varbits.json", varbits)
    file_info["varbits.json"] = {"hash": sha, "entries": count}
    entry_counts["varbits.json"] = count

    # ----- Drift detection -----
    if check_drift(entry_counts):
        drift = True

    # ----- Metadata -----
    write_metadata(file_info)

    print()
    print("=== Summary ===")
    for fname, info in sorted(file_info.items()):
        hash_display = info['hash']
        if hash_display.startswith("sha256:"):
            hash_display = hash_display[7:]
        print(f"  {fname}: {info['entries']} entries (sha256: {hash_display[:16]}...)")

    if drift:
        print()
        print("WARNING: Drift detected. Review entry counts before committing.", file=sys.stderr)
        return 2

    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
