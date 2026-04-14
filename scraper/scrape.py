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

        results = data.get("query", {}).get("results", [])
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
    return str(val)


# ---------------------------------------------------------------------------
# Transformers — one per output file
# ---------------------------------------------------------------------------


def build_items(raw_items: list[dict], raw_bonuses: list[dict]) -> dict:
    """
    Merge infobox_item + infobox_bonuses into ItemDefinition keyed by id string.
    """
    # Index bonuses by item name for merging
    bonuses_by_name: dict[str, dict] = {}
    for b in raw_bonuses:
        name = b.get("item") or b.get("name") or ""
        if name:
            bonuses_by_name[name] = b

    items: dict[str, dict] = {}
    for row in raw_items:
        item_id = safe_int(row.get("id"))
        if item_id is None:
            continue

        name = safe_str(row.get("name"), "")
        bonus = bonuses_by_name.get(name, {})

        equipment = None
        slot = safe_str(bonus.get("slot"))
        if slot:
            # Parse requirement string "{{skill|Attack|40}},{{skill|Defence|20}}" -> {"Attack":40, ...}
            reqs: dict[str, int] = {}
            req_raw = safe_str(bonus.get("requirements"), "")
            if req_raw:
                # Handle both wiki template and plain formats
                for m in re.finditer(r"(\w+)\|(\d+)", req_raw):
                    reqs[m.group(1)] = int(m.group(2))
                # Fallback: "Attack: 40, Defence: 20"
                if not reqs:
                    for m in re.finditer(r"(\w+)\s*[:=]\s*(\d+)", req_raw):
                        reqs[m.group(1)] = int(m.group(2))

            equipment = {
                "slot": slot,
                "attackStab": safe_int(bonus.get("astab"), 0),
                "attackSlash": safe_int(bonus.get("aslash"), 0),
                "attackCrush": safe_int(bonus.get("acrush"), 0),
                "attackMagic": safe_int(bonus.get("amagic"), 0),
                "attackRanged": safe_int(bonus.get("arange"), 0),
                "defenceStab": safe_int(bonus.get("dstab"), 0),
                "defenceSlash": safe_int(bonus.get("dslash"), 0),
                "defenceCrush": safe_int(bonus.get("dcrush"), 0),
                "defenceMagic": safe_int(bonus.get("dmagic"), 0),
                "defenceRanged": safe_int(bonus.get("drange"), 0),
                "meleeStrength": safe_int(bonus.get("str"), 0),
                "rangedStrength": safe_int(bonus.get("rstr"), 0),
                "magicDamage": safe_float(bonus.get("mdmg"), 0.0),
                "prayer": safe_int(bonus.get("prayer"), 0),
                "requirements": reqs,
            }

        weapon = None
        aspeed = safe_int(bonus.get("aspeed"))
        if aspeed is not None:
            weapon = {
                "attackSpeed": aspeed,
                "weaponType": safe_str(bonus.get("combatstyle"), ""),
                "stances": [],  # Stances not available from bucket API
            }

        item = {
            "id": item_id,
            "name": name,
            "members": safe_bool(row.get("members")),
            "tradeable": safe_bool(row.get("tradeable")),
            "tradeableOnGe": safe_bool(row.get("exchange")),
            "stackable": safe_bool(row.get("stackable")),
            "cost": safe_int(row.get("value"), 0),
            "highAlch": safe_int(row.get("highalch")),
            "lowAlch": safe_int(row.get("lowalch")),
            "buyLimit": safe_int(row.get("limit")),
            "weight": safe_float(row.get("weight")),
            "examine": safe_str(row.get("examine")),
            "questItem": safe_bool(row.get("quest")),
            "equipment": equipment,
            "weapon": weapon,
        }

        items[str(item_id)] = item

    return items


def build_monsters(raw: list[dict]) -> dict:
    """MonsterDefinition keyed by id string."""
    monsters: dict[str, dict] = {}
    for row in raw:
        monster_id = safe_int(row.get("id"))
        if monster_id is None:
            continue

        monster = {
            "id": monster_id,
            "name": safe_str(row.get("name"), ""),
            "members": safe_bool(row.get("members")),
            "combatLevel": safe_int(row.get("combat")),
            "hitpoints": safe_int(row.get("hitpoints")),
            "maxHit": safe_str(row.get("max hit")),
            "attackSpeed": safe_int(row.get("attack speed")),
            "size": safe_int(row.get("size")),
            "attackLevel": safe_int(row.get("att")),
            "strengthLevel": safe_int(row.get("str")),
            "defenceLevel": safe_int(row.get("def")),
            "magicLevel": safe_int(row.get("mage")),
            "rangedLevel": safe_int(row.get("range")),
            "attackStab": safe_int(row.get("astab")),
            "attackSlash": safe_int(row.get("aslash")),
            "attackCrush": safe_int(row.get("acrush")),
            "attackMagic": safe_int(row.get("amagic")),
            "attackRanged": safe_int(row.get("arange")),
            "defenceStab": safe_int(row.get("dstab")),
            "defenceSlash": safe_int(row.get("dslash")),
            "defenceCrush": safe_int(row.get("dcrush")),
            "defenceMagic": safe_int(row.get("dmagic")),
            "defenceRanged": safe_int(row.get("drange")),
            "strengthBonus": safe_int(row.get("strbns")),
            "rangedStrengthBonus": safe_int(row.get("rstrbns")),
            "magicDamageBonus": safe_int(row.get("mbns")),
            "slayerLevel": safe_int(row.get("slaylvl")),
            "slayerXp": safe_float(row.get("slayxp")),
            "slayerCategory": safe_str(row.get("cat")),
            "assignedBy": safe_str(row.get("assignedby")),
            "elementalWeakness": safe_str(row.get("elementalweakness")),
            "elementalWeaknessPercent": safe_int(row.get("elementalweaknesspercent")),
            "poisonous": safe_str(row.get("poison")),
            "immunePoison": safe_bool(row.get("immunepoison")) if row.get("immunepoison") not in (None, "") else None,
            "immuneVenom": safe_bool(row.get("immunevenom")) if row.get("immunevenom") not in (None, "") else None,
            "examine": safe_str(row.get("examine")),
        }
        monsters[str(monster_id)] = monster

    return monsters


def build_drops(raw: list[dict]) -> list[dict]:
    """DropEntry as a flat JSON array."""
    drops: list[dict] = []
    for row in raw:
        # Parse rarity — can be "1/128", "Always", a decimal, etc.
        rarity_raw = safe_str(row.get("rarity"), "0")
        rarity = 0.0
        if rarity_raw:
            rarity_lower = rarity_raw.lower().strip()
            if rarity_lower in ("always", "1", "1/1"):
                rarity = 1.0
            elif "/" in rarity_raw:
                parts = rarity_raw.split("/")
                try:
                    rarity = float(parts[0].strip()) / float(parts[1].strip())
                except (ValueError, ZeroDivisionError, IndexError):
                    rarity = 0.0
            else:
                rarity = safe_float(rarity_raw, 0.0)

        drop = {
            "monsterName": safe_str(row.get("monster"), safe_str(row.get("name"), "")),
            "itemName": safe_str(row.get("item"), safe_str(row.get("name2"), "")),
            "itemId": safe_int(row.get("itemid")),
            "quantity": safe_str(row.get("quantity"), "1"),
            "rarity": rarity,
            "noted": safe_bool(row.get("namenotes")),
            "rolls": safe_int(row.get("rolls"), 1),
        }
        drops.append(drop)

    return drops


def build_quests(raw: list[dict]) -> dict:
    """QuestDefinition keyed by quest name."""
    quests: dict[str, dict] = {}
    for row in raw:
        name = safe_str(row.get("name"), "")
        if not name:
            continue

        quest = {
            "name": name,
            "difficulty": safe_str(row.get("difficulty")),
            "length": safe_str(row.get("length")),
            "requirements": safe_str(row.get("requirements")),
            "startPoint": safe_str(row.get("start")),
            "itemsRequired": safe_str(row.get("items")),
            "enemiesToDefeat": safe_str(row.get("kills")),
            "ironmanConcerns": safe_str(row.get("ironman")),
        }
        quests[name] = quest

    return quests


def build_recipes(raw: list[dict]) -> dict:
    """RecipeDefinition keyed by recipe name."""
    recipes: dict[str, dict] = {}
    for row in raw:
        name = safe_str(row.get("name"), "")
        if not name:
            continue

        # Parse materials: "Item1:3,Item2:1" or wiki template style
        materials: list[dict] = []
        mat_raw = safe_str(row.get("mat1"), "")
        # Check mat1..mat10 fields
        for i in range(1, 11):
            mat_name = safe_str(row.get(f"mat{i}"))
            mat_qty = safe_int(row.get(f"mat{i}qty"), 1)
            if mat_name:
                materials.append({"name": mat_name, "quantity": mat_qty})

        # Parse tools and facilities
        tools: list[str] = []
        tools_raw = safe_str(row.get("tools"))
        if tools_raw:
            tools = [t.strip() for t in tools_raw.split(",") if t.strip()]

        facilities: list[str] = []
        fac_raw = safe_str(row.get("facilities"))
        if fac_raw:
            facilities = [f.strip() for f in fac_raw.split(",") if f.strip()]

        recipe = {
            "name": name,
            "members": safe_bool(row.get("members")),
            "skill": safe_str(row.get("skill")),
            "level": safe_int(row.get("level")),
            "experience": safe_float(row.get("experience")),
            "materials": materials,
            "tools": tools,
            "facilities": facilities,
            "boostable": safe_bool(row.get("boostable")),
        }
        recipes[name] = recipe

    return recipes


def build_shops(raw_shops: list[dict], raw_storelines: list[dict]) -> dict:
    """ShopDefinition keyed by shop name. Storelines merged as items."""
    # Group storeline items by shop name
    store_items_by_shop: dict[str, list[dict]] = {}
    for row in raw_storelines:
        shop_name = safe_str(row.get("shop"), safe_str(row.get("store"), ""))
        if not shop_name:
            continue

        item = {
            "itemName": safe_str(row.get("name"), safe_str(row.get("item"), "")),
            "stock": safe_int(row.get("stock")),
            "buyPrice": safe_str(row.get("buy")),
            "sellPrice": safe_str(row.get("sell")),
            "currency": safe_str(row.get("currency")),
            "restockTime": safe_str(row.get("restock")),
        }
        store_items_by_shop.setdefault(shop_name, []).append(item)

    shops: dict[str, dict] = {}
    for row in raw_shops:
        name = safe_str(row.get("name"), "")
        if not name:
            continue

        shop = {
            "name": name,
            "owner": safe_str(row.get("owner")),
            "location": safe_str(row.get("location")),
            "members": safe_bool(row.get("members")),
            "items": store_items_by_shop.get(name, []),
        }
        shops[name] = shop

    return shops


def build_spells(raw: list[dict]) -> dict:
    """SpellDefinition keyed by spell name."""
    spells: dict[str, dict] = {}
    for row in raw:
        name = safe_str(row.get("name"), "")
        if not name:
            continue

        # Parse runes: "Fire rune:5,Air rune:1" or individual rune fields
        runes: dict[str, int] = {}
        for i in range(1, 11):
            rune_name = safe_str(row.get(f"rune{i}"))
            rune_qty = safe_int(row.get(f"rune{i}qty"), 1)
            if rune_name:
                runes[rune_name] = rune_qty

        spell = {
            "name": name,
            "spellbook": safe_str(row.get("spellbook"), "Standard"),
            "level": safe_int(row.get("level"), 1),
            "experience": safe_float(row.get("experience"), 0.0),
            "type": safe_str(row.get("type"), ""),
            "runes": runes,
            "members": safe_bool(row.get("members")),
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

    sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
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

# Fields to request from each bucket. These are the wiki-side column names.
ITEM_FIELDS = [
    "id", "name", "members", "tradeable", "exchange", "stackable",
    "value", "highalch", "lowalch", "limit", "weight", "examine", "quest",
]

BONUSES_FIELDS = [
    "item", "name", "slot",
    "astab", "aslash", "acrush", "amagic", "arange",
    "dstab", "dslash", "dcrush", "dmagic", "drange",
    "str", "rstr", "mdmg", "prayer",
    "aspeed", "combatstyle", "requirements",
]

MONSTER_FIELDS = [
    "id", "name", "members", "combat", "hitpoints",
    "max hit", "attack speed", "size",
    "att", "str", "def", "mage", "range",
    "astab", "aslash", "acrush", "amagic", "arange",
    "dstab", "dslash", "dcrush", "dmagic", "drange",
    "strbns", "rstrbns", "mbns",
    "slaylvl", "slayxp", "cat", "assignedby",
    "elementalweakness", "elementalweaknesspercent",
    "poison", "immunepoison", "immunevenom", "examine",
]

DROP_FIELDS = [
    "monster", "name", "name2", "item", "itemid",
    "quantity", "rarity", "namenotes", "rolls",
]

QUEST_FIELDS = [
    "name", "difficulty", "length", "requirements",
    "start", "items", "kills", "ironman",
]

RECIPE_FIELDS = [
    "name", "members", "skill", "level", "experience",
    "mat1", "mat1qty", "mat2", "mat2qty", "mat3", "mat3qty",
    "mat4", "mat4qty", "mat5", "mat5qty",
    "mat6", "mat6qty", "mat7", "mat7qty", "mat8", "mat8qty",
    "mat9", "mat9qty", "mat10", "mat10qty",
    "tools", "facilities", "boostable",
]

SHOP_FIELDS = [
    "name", "owner", "location", "members",
]

STORELINE_FIELDS = [
    "shop", "store", "name", "item",
    "stock", "buy", "sell", "currency", "restock",
]

SPELL_FIELDS = [
    "name", "spellbook", "level", "experience", "type", "members",
    "rune1", "rune1qty", "rune2", "rune2qty", "rune3", "rune3qty",
    "rune4", "rune4qty", "rune5", "rune5qty",
    "rune6", "rune6qty", "rune7", "rune7qty",
    "rune8", "rune8qty", "rune9", "rune9qty", "rune10", "rune10qty",
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
        print(f"  {fname}: {info['entries']} entries (sha256: {info['hash'][:16]}...)")

    if drift:
        print()
        print("WARNING: Drift detected. Review entry counts before committing.", file=sys.stderr)
        return 2

    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
