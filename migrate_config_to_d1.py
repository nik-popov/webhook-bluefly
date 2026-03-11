"""One-time migration: seed Cloudflare D1 database from config.json."""

import json
import os
import sys

from dotenv import load_dotenv

from d1_client import D1Client


def migrate():
    load_dotenv()

    account_id = os.environ.get("D1_ACCOUNT_ID", "")
    database_id = os.environ.get("D1_DATABASE_ID", "")
    api_token = os.environ.get("D1_API_TOKEN", "")

    if not all([account_id, database_id, api_token]):
        print("ERROR: Set D1_ACCOUNT_ID, D1_DATABASE_ID, and D1_API_TOKEN in .env")
        sys.exit(1)

    client = D1Client(account_id, database_id, api_token)

    # Create table
    client.execute(
        "CREATE TABLE IF NOT EXISTS config ("
        "  key TEXT PRIMARY KEY,"
        "  value TEXT NOT NULL,"
        "  updated_at TEXT DEFAULT (datetime('now'))"
        ")"
    )
    print("Table 'config' ready.")

    # Load existing config
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Insert each key
    for key, value in cfg.items():
        serialized = json.dumps(value) if not isinstance(value, str) else value
        client.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            [key, serialized],
        )
        print(f"  Migrated: {key}")

    print(f"\nDone — migrated {len(cfg)} keys to D1.")


if __name__ == "__main__":
    migrate()
