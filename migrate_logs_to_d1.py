"""Create D1 tables for webhook events, pipeline jobs, and pipeline errors.

Usage:
    python migrate_logs_to_d1.py              # schema only (default)
    python migrate_logs_to_d1.py --migrate-data  # also import existing log files
"""

import glob
import json
import os
import sys

from dotenv import load_dotenv

from d1_client import D1Client

SCHEMA_STATEMENTS = [
    # -- webhook_events --
    """CREATE TABLE IF NOT EXISTS webhook_events (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id          TEXT NOT NULL,
        topic             TEXT NOT NULL,
        shop_domain       TEXT,
        status            TEXT NOT NULL DEFAULT 'unread',
        timestamp         TEXT,
        received_at       TEXT NOT NULL,
        status_updated_at TEXT,
        payload           TEXT,
        created_date      TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_we_status ON webhook_events(status)",
    "CREATE INDEX IF NOT EXISTS idx_we_date ON webhook_events(created_date)",
    "CREATE INDEX IF NOT EXISTS idx_we_event_id ON webhook_events(event_id)",
    "CREATE INDEX IF NOT EXISTS idx_we_status_date ON webhook_events(status, created_date)",
    # -- pipeline_jobs --
    """CREATE TABLE IF NOT EXISTS pipeline_jobs (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id              TEXT NOT NULL UNIQUE,
        source_webhook_file TEXT,
        topic               TEXT NOT NULL,
        product_id          TEXT,
        event_id            TEXT,
        created_at          TEXT NOT NULL,
        status              TEXT NOT NULL DEFAULT 'queued',
        error               TEXT,
        stages              TEXT,
        created_date        TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_pj_status ON pipeline_jobs(status)",
    "CREATE INDEX IF NOT EXISTS idx_pj_date ON pipeline_jobs(created_date)",
    "CREATE INDEX IF NOT EXISTS idx_pj_product ON pipeline_jobs(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_pj_job_id ON pipeline_jobs(job_id)",
    # -- pipeline_errors --
    """CREATE TABLE IF NOT EXISTS pipeline_errors (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id       TEXT NOT NULL,
        product_id   TEXT NOT NULL,
        title        TEXT,
        category_id  TEXT,
        source       TEXT DEFAULT 'dashboard',
        created_at   TEXT NOT NULL,
        status       TEXT NOT NULL DEFAULT 'size_error',
        size_errors  TEXT,
        created_date TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_pe_product ON pipeline_errors(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_pe_date ON pipeline_errors(created_date)",
]


def create_schema(client: D1Client):
    for stmt in SCHEMA_STATEMENTS:
        client.execute(stmt)
        label = stmt.strip().split("\n")[0][:60]
        print(f"  OK: {label}")


def migrate_webhook_events(client: D1Client, log_dir: str):
    pattern = os.path.join(log_dir, "*", "*.json")
    files = sorted(glob.glob(pattern))
    count = 0
    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                rec = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        created_date = rec.get("received_at", "")[:10] or "unknown"
        client.execute(
            "INSERT OR IGNORE INTO webhook_events "
            "(event_id, topic, shop_domain, status, timestamp, received_at, "
            "status_updated_at, payload, created_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                rec.get("event_id", ""),
                rec.get("topic", ""),
                rec.get("shop_domain", ""),
                rec.get("status", "unread"),
                rec.get("timestamp", ""),
                rec.get("received_at", ""),
                rec.get("status_updated_at", ""),
                json.dumps(rec.get("payload", {})),
                created_date,
            ],
        )
        count += 1
    print(f"  Migrated {count} webhook events.")


def migrate_pipeline_jobs(client: D1Client, log_dir: str):
    pattern = os.path.join(log_dir, "*", "*.json")
    files = sorted(glob.glob(pattern))
    count = 0
    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                rec = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        created_date = rec.get("created_at", "")[:10] or "unknown"
        client.execute(
            "INSERT OR IGNORE INTO pipeline_jobs "
            "(job_id, source_webhook_file, topic, product_id, event_id, "
            "created_at, status, error, stages, created_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                rec.get("job_id", ""),
                rec.get("source_webhook_file", ""),
                rec.get("topic", ""),
                str(rec.get("product_id", "")),
                rec.get("event_id", ""),
                rec.get("created_at", ""),
                rec.get("status", "queued"),
                rec.get("error"),
                json.dumps(rec.get("stages", [])),
                created_date,
            ],
        )
        count += 1
    print(f"  Migrated {count} pipeline jobs.")


def migrate_pipeline_errors(client: D1Client, error_dir: str):
    pattern = os.path.join(error_dir, "*", "*.json")
    files = sorted(glob.glob(pattern))
    count = 0
    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                rec = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        created_date = rec.get("created_at", "")[:10] or "unknown"
        client.execute(
            "INSERT OR IGNORE INTO pipeline_errors "
            "(job_id, product_id, title, category_id, source, "
            "created_at, status, size_errors, created_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                rec.get("job_id", ""),
                str(rec.get("product_id", "")),
                rec.get("title", ""),
                rec.get("category_id", ""),
                rec.get("source", "dashboard"),
                rec.get("created_at", ""),
                rec.get("status", "size_error"),
                json.dumps(rec.get("size_errors", [])),
                created_date,
            ],
        )
        count += 1
    print(f"  Migrated {count} pipeline errors.")


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

    if "--migrate-data" in sys.argv:
        base = os.path.dirname(__file__)
        log_dir = os.environ.get("LOG_DIR", os.path.join(base, "logs"))
        pipeline_dir = os.environ.get(
            "PIPELINE_LOG_DIR", os.path.join(base, "pipeline_logs")
        )
        error_dir = os.path.join(base, "pipeline_logs_errors")

        print("Migrating webhook events...")
        migrate_webhook_events(client, log_dir)

        print("Migrating pipeline jobs...")
        migrate_pipeline_jobs(client, pipeline_dir)

        print("Migrating pipeline errors...")
        migrate_pipeline_errors(client, error_dir)

        print("\nData migration complete.")
    else:
        print("Run with --migrate-data to also import existing log files.")


if __name__ == "__main__":
    main()
