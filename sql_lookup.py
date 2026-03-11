"""
Bluefly category-specific field mappings via Cloudflare D1.

Queries bf_categories and bf_mapping tables to get the Bluefly field names
and values for a given Shopify category + variant size.
"""

import threading
import time

from d1_client import D1Client

# Module-level TTL cache for lookups (shared across instances)
_sql_cache: dict[tuple, tuple[float, dict]] = {}
_sql_cache_lock = threading.Lock()
_SQL_CACHE_TTL = 300  # 5 minutes


class BlueflyDBLookup:
    def __init__(self, d1_client: D1Client):
        self._d1 = d1_client

    def lookup_category_fields(
        self, category_id: str, variant_title: str
    ) -> dict[str, str]:
        """
        Look up Bluefly field mappings for a category + variant size.

        Returns:
            Dict of {FieldName: BFValue} for the matching category + size.
            Empty dict if no matches found.
        """
        rows = self._d1.execute(
            """
            SELECT m.field_name, m.bf_value
            FROM bf_categories c
            INNER JOIN bf_mapping m ON c.field_name = m.field_name
            WHERE c.category_id = ?
            AND m.sh_value = ?
            """,
            [category_id, variant_title],
        )
        results = {}
        for row in rows:
            results[row["field_name"]] = row["bf_value"]
        print(f"    [D1] cat={category_id} variant='{variant_title}' -> {len(rows)} rows")
        return results

    def lookup_category_fields_batch(
        self, category_id: str, variant_titles: list[str]
    ) -> dict[str, dict[str, str]]:
        """
        Look up Bluefly field mappings for a category + multiple variant sizes
        in a single query with TTL cache.

        Returns:
            Dict of {variant_title: {FieldName: BFValue}} for all matching variants.
        """
        if not variant_titles:
            return {}

        cache_key = (category_id, frozenset(variant_titles))
        now = time.time()

        with _sql_cache_lock:
            if cache_key in _sql_cache:
                ts, cached_result = _sql_cache[cache_key]
                if now - ts < _SQL_CACHE_TTL:
                    return cached_result.copy()

        placeholders = ",".join("?" for _ in variant_titles)
        rows = self._d1.execute(
            f"""
            SELECT m.field_name, m.bf_value, m.sh_value
            FROM bf_categories c
            INNER JOIN bf_mapping m ON c.field_name = m.field_name
            WHERE c.category_id = ?
            AND m.sh_value IN ({placeholders})
            """,
            [category_id] + list(variant_titles),
        )
        results = {}
        for row in rows:
            vt = row["sh_value"]
            if vt not in results:
                results[vt] = {}
            results[vt][row["field_name"]] = row["bf_value"]
        print(f"    [D1] cat={category_id} batch={len(variant_titles)} variants -> {len(rows)} rows")

        with _sql_cache_lock:
            _sql_cache[cache_key] = (now, results)

        return results

    def test_connection(self) -> bool:
        """Quick connectivity check against D1."""
        try:
            self._d1.execute("SELECT 1 as ok")
            return True
        except Exception:
            return False
