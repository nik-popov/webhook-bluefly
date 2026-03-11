"""Migrate overrides from config blob to dedicated D1 tables + backfill products table.

Usage:
    python migrate_overrides_to_d1.py              # create tables + migrate data
    python migrate_overrides_to_d1.py --schema-only # create tables only
"""

import json
import os
import sys

from dotenv import load_dotenv

from d1_client import D1Client

SCHEMA_STATEMENTS = [
    # -- Simple override tables --
    """CREATE TABLE IF NOT EXISTS override_category (
        product_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS override_size_field (
        product_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS override_title (
        product_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS override_vendor (
        product_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS override_brand_product_id (
        product_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS override_price (
        product_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    # -- Image overrides (composite key) --
    """CREATE TABLE IF NOT EXISTS override_image (
        product_id TEXT NOT NULL,
        image_index INTEGER NOT NULL,
        url TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (product_id, image_index)
    )""",
    # -- Products sync status --
    """CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        sync_status TEXT NOT NULL DEFAULT 'never',
        sync_time TEXT,
        sku TEXT,
        size_errors TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    "CREATE INDEX IF NOT EXISTS idx_products_status ON products(sync_status)",
]

# Map config keys to D1 table names
OVERRIDE_KEY_MAP = {
    "category_overrides": "override_category",
    "size_field_overrides": "override_size_field",
    "title_overrides": "override_title",
    "vendor_overrides": "override_vendor",
    "brand_product_id_overrides": "override_brand_product_id",
    "price_overrides": "override_price",
}


def create_schema(client: D1Client):
    for stmt in SCHEMA_STATEMENTS:
        client.execute(stmt)
        label = stmt.strip().split("\n")[0][:60]
        print(f"  OK: {label}")


def migrate_simple_overrides(client: D1Client):
    """Read override blobs from config table, insert into dedicated tables."""
    # Load all config
    rows = client.execute("SELECT key, value FROM config")
    cfg = {}
    for row in rows:
        try:
            cfg[row["key"]] = json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            cfg[row["key"]] = row["value"]

    for config_key, table_name in OVERRIDE_KEY_MAP.items():
        overrides = cfg.get(config_key, {})
        if not isinstance(overrides, dict):
            print(f"  Skipping {config_key} (not a dict)")
            continue
        count = 0
        for product_id, value in overrides.items():
            serialized = json.dumps(value) if not isinstance(value, str) else value
            client.execute(
                f"INSERT OR REPLACE INTO {table_name} (product_id, value) VALUES (?, ?)",
                [str(product_id), serialized],
            )
            count += 1
        print(f"  {config_key} -> {table_name}: {count} rows")

    # Image overrides (nested: {pid: {idx: url}})
    image_overrides = cfg.get("image_overrides", {})
    count = 0
    if isinstance(image_overrides, dict):
        for product_id, indices in image_overrides.items():
            if not isinstance(indices, dict):
                continue
            for idx_str, url in indices.items():
                client.execute(
                    "INSERT OR REPLACE INTO override_image "
                    "(product_id, image_index, url) VALUES (?, ?, ?)",
                    [str(product_id), int(idx_str), url],
                )
                count += 1
    print(f"  image_overrides -> override_image: {count} rows")


def backfill_products_table(client: D1Client):
    """Build products table from pipeline_jobs (same logic as get_synced_product_ids)."""
    rows = client.execute(
        "SELECT product_id, status, created_at, stages FROM pipeline_jobs ORDER BY id ASC"
    )
    result = {}
    for row in rows:
        pid = row.get("product_id")
        stages = []
        try:
            stages = json.loads(row.get("stages", "[]") or "[]")
        except (json.JSONDecodeError, TypeError):
            pass

        # Prefer enriched-stage product_id for inventory webhooks
        for stage in stages:
            if stage.get("stage") == "enriched":
                enriched_pid = (stage.get("data") or {}).get("product_id")
                if enriched_pid:
                    pid = enriched_pid
                break

        if pid:
            pid = str(pid)
            entry = {"status": row.get("status", "unknown"), "time": row.get("created_at")}
            sku = None
            size_errors = None
            for stage in stages:
                if stage.get("stage") in ("pushed", "size_error"):
                    se = (stage.get("data") or {}).get("size_errors")
                    if se:
                        size_errors = se
                    s = (stage.get("data") or {}).get("sku")
                    if s:
                        sku = s
                    break
            result[pid] = {**entry, "sku": sku, "size_errors": size_errors}

    count = 0
    for pid, info in result.items():
        se_json = json.dumps(info["size_errors"]) if info.get("size_errors") else None
        client.execute(
            "INSERT OR REPLACE INTO products "
            "(product_id, sync_status, sync_time, sku, size_errors) "
            "VALUES (?, ?, ?, ?, ?)",
            [pid, info["status"], info.get("time"), info.get("sku"), se_json],
        )
        count += 1
    print(f"  Backfilled {count} products from pipeline_jobs.")


def cleanup_config(client: D1Client):
    """Remove migrated override keys from config table."""
    keys_to_remove = list(OVERRIDE_KEY_MAP.keys()) + ["image_overrides"]
    for key in keys_to_remove:
        client.execute("DELETE FROM config WHERE key = ?", [key])
        print(f"  Removed config key: {key}")


def main():
    load_dotenv()

    account_id = os.environ.get("D1_ACCOUNT_ID", "")
    database_id = os.environ.get("D1_DATABASE_ID", "")
    api_token = os.environ.get("D1_API_TOKEN", "")

    if not all([account_id, database_id, api_token]):
        print("ERROR: Set D1_ACCOUNT_ID, D1_DATABASE_ID, and D1_API_TOKEN in .env")
        sys.exit(1)

    client = D1Client(account_id, database_id, api_token)

    print("Creating tables...")
    create_schema(client)
    print("Schema ready.\n")

    if "--schema-only" in sys.argv:
        print("Schema-only mode. Done.")
        return

    print("Migrating simple overrides...")
    migrate_simple_overrides(client)

    print("\nBackfilling products table...")
    backfill_products_table(client)

    print("\nCleaning up config table...")
    cleanup_config(client)

    print("\nMigration complete.")


if __name__ == "__main__":
    main()
