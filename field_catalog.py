"""
Local Bluefly field catalog — parses bluefly_field_catalog.tsv to resolve
category-specific size fields and validate allowed values.

Replaces the SQL Server lookup (sql_lookup.py) for size field resolution.
"""

import csv
import json
import os
import re

_TSV_PATH = os.path.join(os.path.dirname(__file__), "data", "bluefly_field_catalog.tsv")
from d1_client import get_config_store


def _parse_numeric_keys(d):
    """Convert string keys to float (or int where possible) for numeric lookup tables."""
    out = {}
    for k, v in d.items():
        try:
            n = float(k)
            out[int(n) if n == int(n) else n] = v
        except ValueError:
            out[k] = v
    return out


class BlueflyFieldCatalog:
    """Parse the field catalog TSV and resolve size fields by category ID."""

    def __init__(self, tsv_path: str = _TSV_PATH):
        self._fields = []  # list of parsed rows
        self._size_fields = []  # subset: only ConditionallyRequired size fields
        self._load(tsv_path)
        self._brand_conversions = self._load_brand_conversions()

    def _load(self, tsv_path: str):
        with open(tsv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                self._fields.append(row)
                # Size fields are ConditionallyRequired and have Target categories
                if row.get("Importance") == "ConditionallyRequired" and row.get("Target"):
                    self._size_fields.append(row)

    @staticmethod
    def _load_brand_conversions():
        """Load brand_size_conversions from config (label -> {size: value})."""
        try:
            cfg = get_config_store().load()
            raw = cfg.get("brand_size_conversions", {})
            # Parse numeric keys for each brand table
            return {label: _parse_numeric_keys(table) for label, table in raw.items()}
        except Exception:
            return {}

    def infer_size_field_from_scale(self, scale_label: str) -> str | None:
        """
        Infer the Bluefly size field name from an ff_size_scale label.

        Uses keyword matching on the label.
        Returns a field name like 'size_shoes', or 'ONE_SIZE' sentinel, or None.
        """
        if not scale_label:
            return None
        # Keyword matching on the label
        low = scale_label.lower()
        if "one size" in low or low == "one_size":
            return "ONE_SIZE"
        if "lingerie" in low:
            return "size_bras"
        if "shoe" in low:
            return "size_shoes"
        if "clothing" in low or "standard" in low:
            if "women" in low or "woman" in low:
                return "size_womens_clothing"
            return "size_s_m_l"
        return None

    def get_size_field_for_category(self, category_id: str) -> dict | None:
        """
        Given a Bluefly category ID, return the matching size field definition.

        Returns dict with keys: FieldName, AllowedValues (list), FieldDisplayName
        or None if no size field applies to this category.
        """
        if not category_id:
            return None
        cat = str(category_id).strip()
        for field in self._size_fields:
            targets = field.get("Target", "").split("|^|")
            if cat in targets:
                allowed_raw = field.get("AllowedValues", "")
                allowed = [v.strip() for v in allowed_raw.split("|^|") if v.strip()]
                return {
                    "field_name": field["FieldName"],
                    "display_name": field["FieldDisplayName"],
                    "allowed_values": allowed,
                }
        return None

    def get_size_field_by_name(self, field_name: str) -> dict | None:
        """Look up a size field by its FieldName (e.g. 'size_shoes')."""
        for field in self._size_fields:
            if field["FieldName"] == field_name:
                allowed_raw = field.get("AllowedValues", "")
                allowed = [v.strip() for v in allowed_raw.split("|^|") if v.strip()]
                return {
                    "field_name": field["FieldName"],
                    "display_name": field["FieldDisplayName"],
                    "allowed_values": allowed,
                }
        return None

    def list_size_fields(self) -> list[dict]:
        """Return list of all distinct size field definitions [{field_name, display_name}, ...]."""
        seen = set()
        result = []
        for field in self._size_fields:
            fn = field["FieldName"]
            if fn not in seen:
                seen.add(fn)
                result.append({
                    "field_name": fn,
                    "display_name": field["FieldDisplayName"],
                    "target_ids": [t.strip() for t in field.get("Target", "").split("|^|") if t.strip()],
                })
        return sorted(result, key=lambda x: x["display_name"])

    def resolve_size(self, category_id: str, shopify_size: str, size_scale: str = None, size_field_override: str = None) -> dict:
        """
        Resolve a Shopify size value to the correct Bluefly field name and value.

        Args:
            category_id: Bluefly category ID for field lookup
            shopify_size: Raw size value from Shopify variant
            size_scale: Optional ff_size_scale label used as tiebreaker when
                        category-based matching fails

        Returns:
            {"field_name": "size_shoes", "value": "9.5"} on match
            {"field_name": "size_shoes", "value": None, "error": "..."} if no match
            {"field_name": None, "value": None} if category has no size field
        """
        # Use size field override if provided, otherwise resolve from category
        if size_field_override:
            size_field = self.get_size_field_by_name(size_field_override)
        else:
            size_field = self.get_size_field_for_category(category_id)
        if not size_field:
            return {"field_name": None, "value": None}

        field_name = size_field["field_name"]
        allowed = size_field["allowed_values"]
        raw = shopify_size.strip()

        # Normalize dual EU/US formats — extract the US size when both are present
        # Also preserve EU/IT/UK value for brand table lookup
        eu_it_value = None
        # Pattern A: "EU 39 /US 6", "EU 41/ US 8"
        dual_match = re.match(r'EU\s*([\d.]+)\s*/\s*US\s*([\d.]+)', raw, re.IGNORECASE)
        # Pattern B: "42.5 EU / 9.5 US" — number-first with slash
        if not dual_match:
            dual_match = re.match(r'([\d.]+)\s*EU\s*/\s*([\d.]+)\s*US', raw, re.IGNORECASE)
        # Pattern C: "7 UK - 9 US" or "9UK - 11US" — UK-dash-US format
        if not dual_match:
            dual_match = re.match(r'([\d.]+)\s*UK\s*[-/]\s*([\d.]+)\s*US', raw, re.IGNORECASE)
        # Pattern D: "7 UK - 8/8.5 US" — UK with dual US sizes, take first
        if not dual_match:
            dual_match = re.match(r'([\d.]+)\s*UK\s*[-/]\s*([\d.]+)/[\d.]+\s*US', raw, re.IGNORECASE)
        # Pattern E: "42 (16.5)" or "40 (15 3/4)" — EU with parenthetical US/inch equivalent
        paren_parsed = False
        if not dual_match:
            paren_match = re.match(r'^([\d.]+)\s*\((.+?)\)\s*$', raw)
            if paren_match:
                eu_it_value = paren_match.group(1)
                inner = paren_match.group(2).strip()
                # Handle fraction inside parens: "15 3/4" → "15.75"
                frac_inner = re.match(r'^(\d+)\s+(\d+)/(\d+)$', inner)
                if frac_inner:
                    whole = int(frac_inner.group(1))
                    numer = int(frac_inner.group(2))
                    denom = int(frac_inner.group(3))
                    inner = str(whole + numer / denom) if denom else inner
                raw = inner
                paren_parsed = True
        if dual_match:
            eu_it_value = dual_match.group(1)  # EU/IT/UK portion
            raw = dual_match.group(2)          # US portion
        elif not paren_parsed:
            # Strip trailing region suffixes like " EU", " US", " UK"
            raw = re.sub(r'\s+(EU|US|UK)$', '', raw, flags=re.IGNORECASE)
            # Strip leading "EU " or "US " prefix (e.g. "EU 42")
            raw = re.sub(r'^(EU|US|UK)\s+', '', raw, flags=re.IGNORECASE)
            # Strip trailing "+" (e.g. "39+")
            raw = raw.rstrip('+')
            # Convert fraction format: "38 1/2" → "38.5"
            frac_match = re.match(r'^(\d+)\s+(\d+)/(\d+)$', raw)
            if frac_match:
                whole = int(frac_match.group(1))
                numer = int(frac_match.group(2))
                denom = int(frac_match.group(3))
                if denom:
                    raw = str(whole + numer / denom)

        if not raw:
            return {"field_name": field_name, "value": None,
                    "error": "Empty size value"}

        # 0) Exact match — try raw (US/inch) first, then eu_it_value fallback
        _has_brand_table = size_scale and size_scale in self._brand_conversions
        _brand_converted = False  # tracks whether brand table actually matched
        _brand_converted_val = None  # the US value after brand conversion (for bf_mapping fallback)
        for av in allowed:
            if av.lower() == raw.lower():
                return {"field_name": field_name, "value": av}
        # Fallback: if dual format parsed, try the EU/IT portion too
        if eu_it_value:
            for av in allowed:
                if av.lower() == eu_it_value.lower():
                    return {"field_name": field_name, "value": av}

        # 1) Brand-specific conversion — convert IT/UK → US only if original didn't match
        if _has_brand_table:
            brand_table = self._brand_conversions[size_scale]
            # Use EU/IT/UK value for brand lookup when dual-format was parsed
            brand_lookup_raw = eu_it_value if eu_it_value else raw
            try:
                lookup_key = float(brand_lookup_raw)
                lookup_key = int(lookup_key) if lookup_key == int(lookup_key) else lookup_key
            except ValueError:
                lookup_key = brand_lookup_raw
            converted = brand_table.get(lookup_key)
            if converted:
                _brand_converted = True
                us_val = str(converted)
                _brand_converted_val = us_val
                src_label = str(lookup_key)
                # Try exact match of US value against allowed values
                for av in allowed:
                    if av.lower() == us_val.lower():
                        return {"field_name": field_name, "value": av,
                                "brand_converted": f"{src_label} -> {us_val}"}
                # Try with "Medium" width suffix for shoes
                for av in allowed:
                    if av == f"{us_val} Medium":
                        return {"field_name": field_name, "value": av,
                                "brand_converted": f"{src_label} -> {us_val}"}
                # Converted US value didn't match — fall through to other steps

        # 2) Numeric match — Shopify often sends "9" but Bluefly wants "9" or "9.5"
        #    Also handles "9.0" → "9", "41.0" → "41"
        try:
            num = float(raw)
            # Try exact numeric string
            for av in allowed:
                try:
                    if float(av.split()[0]) == num:
                        # If it's just a number (no width suffix), prefer plain match
                        if av == av.split()[0]:
                            return {"field_name": field_name, "value": av}
                except (ValueError, IndexError):
                    continue
            # Exact numeric not found; try integer form
            if num == int(num):
                int_str = str(int(num))
                for av in allowed:
                    if av == int_str:
                        return {"field_name": field_name, "value": av}
        except ValueError:
            pass

        # 3) Prefix match for width variants — "9" matches "9 Medium" (default width)
        #    Skip "x NN" suffixes (waist x inseam) — "40" must NOT match "40 x 28"
        for av in allowed:
            parts = av.split(None, 1)
            if len(parts) == 2 and parts[0].lower() == raw.lower():
                suffix = parts[1].strip().lower()
                if suffix.startswith("x "):
                    continue  # waist x inseam — not a width match
                if suffix == "medium":
                    return {"field_name": field_name, "value": av}
        # If no Medium width, take first non-"x" prefix match
        for av in allowed:
            parts = av.split(None, 1)
            if len(parts) == 2 and parts[0].lower() == raw.lower():
                suffix = parts[1].strip().lower()
                if suffix.startswith("x "):
                    continue
                return {"field_name": field_name, "value": av}

        # 4) SML shorthand expansion — "S" → "Regular S", "XL" → "Regular XL"
        sml_map = {
            "xxs": "Regular XXS", "xs": "Regular XS", "s": "Regular S",
            "m": "Regular M", "l": "Regular L", "xl": "Regular XL",
            "xxl": "Regular XXL", "xxxl": "Regular XXXL", "xxxs": "Regular XXXS",
            "small": "Regular S", "medium": "Regular M", "large": "Regular L",
        }
        expanded = sml_map.get(raw.lower())
        if expanded:
            for av in allowed:
                if av.lower() == expanded.lower():
                    return {"field_name": field_name, "value": av}

        # 4b) SML parenthetical match — "S" → "Regular 4 (S)" in size_womens_clothing
        sml_suffix = raw.upper()
        if sml_suffix in ("XXS", "XS", "S", "M", "L", "XL", "XXL", "XXXL"):
            for av in allowed:
                if av.startswith("Regular ") and av.endswith(f"({sml_suffix})"):
                    return {"field_name": field_name, "value": av}

        # 5) One Size matching
        if raw.lower() in ("one size", "os", "o/s", "one size fits all", "osfa"):
            for av in allowed:
                if "one size" in av.lower() or av in ("OS", "OSFA"):
                    return {"field_name": field_name, "value": av}

        # 6) Waist size — "30" → '30" Waist'
        if field_name == "size_waist":
            try:
                waist_num = int(float(raw))
                waist_str = f'{waist_num}" Waist'
                for av in allowed:
                    if av == waist_str:
                        return {"field_name": field_name, "value": av}
            except ValueError:
                pass

        # 7) Women's clothing numeric — "6" should match "6" or "Regular 6 (S)"
        if field_name == "size_womens_clothing":
            for av in allowed:
                if av == raw:
                    return {"field_name": field_name, "value": av}
            # Try "Regular N (X)" pattern
            for av in allowed:
                if av.startswith(f"Regular {raw} "):
                    return {"field_name": field_name, "value": av}

        # 8) Suit size — plain number "46" → "46r" (regular fit default)
        if field_name == "size_suits":
            regular = raw.lower() + "r"
            for av in allowed:
                if av.lower() == regular:
                    return {"field_name": field_name, "value": av}

        # Tiebreaker: if category-based field didn't match, try ff_size_scale hint
        if size_scale:
            hint_field = self.infer_size_field_from_scale(size_scale)
            if hint_field == "ONE_SIZE":
                for av in allowed:
                    if "one size" in av.lower() or av in ("OS", "OSFA"):
                        return {"field_name": field_name, "value": av, "hint_used": size_scale}
            elif hint_field and hint_field != field_name:
                # Try resolving against the hinted field using one of its target categories
                for sf in self._size_fields:
                    if sf["FieldName"] == hint_field:
                        hint_cats = sf.get("Target", "").split("|^|")
                        if hint_cats:
                            # Recursive call without size_scale to prevent infinite loop
                            result = self.resolve_size(hint_cats[0], shopify_size)
                            if result.get("value"):
                                result["hint_used"] = size_scale
                                return result
                        break

        # Cross-check: would this value match in a different size field?
        suggestion = self._find_matching_size_field(raw, field_name)
        result = {
            "field_name": field_name,
            "value": None,
            "error": f"No match for '{raw}' in {field_name} ({len(allowed)} allowed values)",
        }
        if _brand_converted_val:
            result["brand_converted_raw"] = _brand_converted_val
        if suggestion:
            result["suggestion"] = suggestion
        return result

    def _find_matching_size_field(self, raw: str, current_field: str) -> str | None:
        """Check if raw size value matches in any other size field (case-insensitive).
        Returns a suggestion string listing all matching fields."""
        matches = []
        for field in self._size_fields:
            fname = field["FieldName"]
            if fname == current_field:
                continue
            allowed_raw = field.get("AllowedValues", "")
            allowed = [v.strip() for v in allowed_raw.split("|^|") if v.strip()]
            found = False
            for av in allowed:
                if av.lower() == raw.lower():
                    found = True
                    break
            if not found:
                try:
                    num = float(raw)
                    num_str = str(int(num)) if num == int(num) else str(num)
                    for av in allowed:
                        if av == num_str:
                            found = True
                            break
                except ValueError:
                    pass
            if found:
                cats = [t for t in field.get("Target", "").split("|^|") if t]
                matches.append(f"{fname} (cat {', '.join(cats[:3])}{'...' if len(cats) > 3 else ''})")
        if not matches:
            return None
        return f"Possibly miscategorized — '{raw}' exists in: {'; '.join(matches)}"

    def get_all_size_field_names(self) -> list[str]:
        """Return all possible size field internal names."""
        return [f["FieldName"] for f in self._size_fields]

    def get_field_info(self, field_name: str) -> dict | None:
        """Look up a field by its FieldName (internal name)."""
        for f in self._fields:
            if f.get("FieldName") == field_name:
                return f
        return None


# Module-level singleton for convenience
_catalog = None


def get_catalog() -> BlueflyFieldCatalog:
    """Return a module-level singleton catalog instance."""
    global _catalog
    if _catalog is None:
        _catalog = BlueflyFieldCatalog()
    return _catalog
