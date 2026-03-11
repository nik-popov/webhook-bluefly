"""Product override and sync status storage backed by Cloudflare D1.

Tables:
  override_category, override_size_field, override_title,
  override_vendor, override_brand_product_id, override_price,
  override_image, products
"""

import json
import logging
import threading

logger = logging.getLogger(__name__)

# Valid override table names (used to prevent SQL injection)
_OVERRIDE_TABLES = {
    "category": "override_category",
    "size_field": "override_size_field",
    "title": "override_title",
    "vendor": "override_vendor",
    "brand_product_id": "override_brand_product_id",
    "price": "override_price",
    "gender": "override_gender",
}


def _table(override_type: str) -> str:
    t = _OVERRIDE_TABLES.get(override_type)
    if not t:
        raise ValueError(f"Unknown override type: {override_type}")
    return t


class ProductStore:
    """D1-backed storage for per-product overrides and sync status."""

    def __init__(self, d1_client):
        self._d1 = d1_client

    # ---- Simple overrides (category, size_field, title, vendor, brand_product_id, price) ----

    def get_all_overrides(self, override_type: str) -> dict:
        """Return {product_id: value} for all rows."""
        table = _table(override_type)
        rows = self._d1.execute(f"SELECT product_id, value FROM {table}")
        return {r["product_id"]: r["value"] for r in rows}

    def get_override(self, override_type: str, product_id: str) -> str | None:
        table = _table(override_type)
        rows = self._d1.execute(
            f"SELECT value FROM {table} WHERE product_id = ?", [product_id]
        )
        return rows[0]["value"] if rows else None

    def set_all_overrides(self, override_type: str, data: dict) -> None:
        """Replace all overrides of a type with the given dict (1 batch call)."""
        table = _table(override_type)
        statements = [(f"DELETE FROM {table}", None)]
        for pid, val in data.items():
            serialized = json.dumps(val) if not isinstance(val, str) else val
            statements.append((
                f"INSERT OR REPLACE INTO {table} (product_id, value, updated_at) "
                "VALUES (?, ?, datetime('now'))",
                [pid, serialized],
            ))
        self._d1.execute_batch(statements)

    def set_override(self, override_type: str, product_id: str, value: str) -> None:
        table = _table(override_type)
        serialized = json.dumps(value) if not isinstance(value, str) else value
        self._d1.execute(
            f"INSERT OR REPLACE INTO {table} (product_id, value, updated_at) "
            "VALUES (?, ?, datetime('now'))",
            [product_id, serialized],
        )

    def delete_override(self, override_type: str, product_id: str) -> None:
        table = _table(override_type)
        self._d1.execute(f"DELETE FROM {table} WHERE product_id = ?", [product_id])

    # ---- Image overrides (composite key: product_id + image_index) ----

    def get_all_image_overrides(self) -> dict:
        """Return {product_id: {image_index_str: url}}."""
        rows = self._d1.execute(
            "SELECT product_id, image_index, url FROM override_image"
        )
        result = {}
        for r in rows:
            pid = r["product_id"]
            if pid not in result:
                result[pid] = {}
            result[pid][str(r["image_index"])] = r["url"]
        return result

    def get_image_overrides(self, product_id: str) -> dict:
        """Return {image_index_str: url} for one product."""
        rows = self._d1.execute(
            "SELECT image_index, url FROM override_image WHERE product_id = ?",
            [product_id],
        )
        return {str(r["image_index"]): r["url"] for r in rows}

    def set_image_override(self, product_id: str, image_index: int, url: str) -> None:
        self._d1.execute(
            "INSERT OR REPLACE INTO override_image "
            "(product_id, image_index, url, updated_at) VALUES (?, ?, ?, datetime('now'))",
            [product_id, image_index, url],
        )

    def delete_image_override(self, product_id: str, image_index: int) -> None:
        self._d1.execute(
            "DELETE FROM override_image WHERE product_id = ? AND image_index = ?",
            [product_id, image_index],
        )

    def set_all_image_overrides(self, data: dict) -> None:
        """Replace all image overrides (1 batch call)."""
        statements = [("DELETE FROM override_image", None)]
        for pid, indices in data.items():
            for idx_str, url in indices.items():
                statements.append((
                    "INSERT INTO override_image "
                    "(product_id, image_index, url, updated_at) VALUES (?, ?, ?, datetime('now'))",
                    [pid, int(idx_str), url],
                ))
        self._d1.execute_batch(statements)

    # ---- Batch loading (anti-N+1 for product listing) ----

    def load_all_overrides_batch(self) -> dict:
        """Load all override types in 1 batch call. Returns dict keyed by type."""
        override_keys = list(_OVERRIDE_TABLES.keys())
        statements = []
        for otype in override_keys:
            table = _OVERRIDE_TABLES[otype]
            statements.append((f"SELECT product_id, value FROM {table}", None))
        statements.append(
            ("SELECT product_id, image_index, url FROM override_image", None)
        )

        try:
            batch_results = self._d1.execute_batch(statements)
        except Exception:
            return {otype: {} for otype in override_keys} | {"image": {}}

        result = {}
        for i, otype in enumerate(override_keys):
            try:
                rows = batch_results[i]
                result[otype] = {r["product_id"]: r["value"] for r in rows}
            except Exception:
                result[otype] = {}

        # Image overrides (last statement)
        try:
            img_rows = batch_results[len(override_keys)]
            images = {}
            for r in img_rows:
                pid = r["product_id"]
                if pid not in images:
                    images[pid] = {}
                images[pid][str(r["image_index"])] = r["url"]
            result["image"] = images
        except Exception:
            result["image"] = {}

        return result

    def get_all_overrides_for_product(self, product_id: str) -> dict:
        """Fetch all override types for one product in 1 batch call.

        Returns: {category: val|None, size_field: ..., ..., image: {idx: url}}
        """
        override_keys = list(_OVERRIDE_TABLES.keys())
        statements = []
        for otype in override_keys:
            table = _OVERRIDE_TABLES[otype]
            statements.append(
                (f"SELECT value FROM {table} WHERE product_id = ?", [product_id])
            )
        statements.append((
            "SELECT image_index, url FROM override_image WHERE product_id = ?",
            [product_id],
        ))

        batch_results = self._d1.execute_batch(statements)

        result = {}
        for i, otype in enumerate(override_keys):
            rows = batch_results[i]
            result[otype] = rows[0]["value"] if rows else None

        img_rows = batch_results[len(override_keys)]
        result["image"] = {str(r["image_index"]): r["url"] for r in img_rows}

        return result

    # ---- Products table (sync status) ----

    def get_all_product_statuses(self) -> dict:
        """Return {product_id: {status, time, size_errors}} matching
        the shape of the old get_synced_product_ids().
        """
        rows = self._d1.execute(
            "SELECT product_id, sync_status, sync_time, sku, size_errors "
            "FROM products"
        )
        result = {}
        for r in rows:
            entry = {
                "status": r["sync_status"],
                "time": r.get("sync_time"),
            }
            se_raw = r.get("size_errors")
            if se_raw:
                try:
                    entry["size_errors"] = json.loads(se_raw)
                except (json.JSONDecodeError, TypeError):
                    pass
            result[r["product_id"]] = entry
        return result

    def get_product_status(self, product_id: str) -> dict | None:
        """Return {sync_status, sync_time, sku, size_errors} or None."""
        rows = self._d1.execute(
            "SELECT sync_status, sync_time, sku, size_errors FROM products WHERE product_id = ?",
            [product_id],
        )
        if not rows:
            return None
        r = rows[0]
        entry = {
            "sync_status": r["sync_status"],
            "sync_time": r.get("sync_time"),
            "sku": r.get("sku"),
        }
        se_raw = r.get("size_errors")
        if se_raw:
            try:
                entry["size_errors"] = json.loads(se_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        return entry

    def upsert_product_status(
        self,
        product_id: str,
        sync_status: str,
        sku: str = None,
        size_errors: list = None,
    ) -> None:
        se_json = json.dumps(size_errors) if size_errors else None
        self._d1.execute(
            "INSERT OR REPLACE INTO products "
            "(product_id, sync_status, sync_time, sku, size_errors, updated_at) "
            "VALUES (?, ?, datetime('now'), ?, ?, datetime('now'))",
            [product_id, sync_status, sku, se_json],
        )

    def delete_product_statuses(self, product_ids: set[str]) -> int:
        if not product_ids:
            return 0
        pids = list(product_ids)
        CHUNK = 500
        for i in range(0, len(pids), CHUNK):
            chunk = pids[i : i + CHUNK]
            placeholders = ", ".join("?" for _ in chunk)
            self._d1.execute(
                f"DELETE FROM products WHERE product_id IN ({placeholders})",
                chunk,
            )
        return len(pids)

    def reset_all_product_statuses(self) -> int:
        rows = self._d1.execute("SELECT COUNT(*) as cnt FROM products")
        count = rows[0]["cnt"] if rows else 0
        self._d1.execute("DELETE FROM products")
        return count


# ---- Module-level singleton ----

_store: ProductStore | None = None
_store_lock = threading.Lock()


def get_product_store() -> ProductStore:
    """Return the module-level ProductStore singleton."""
    global _store
    if _store is not None:
        return _store
    with _store_lock:
        if _store is not None:
            return _store
        from d1_client import get_d1_client
        _store = ProductStore(get_d1_client())
        return _store
