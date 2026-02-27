import os
import json
import glob
from datetime import datetime, timezone

from filelock import FileLock


# Valid status transitions
STATUSES = ("unread", "read", "processing", "processed", "error")


class TransactionLogger:
    """
    Logs each webhook event as an individual JSON file with status tracking.

    Directory layout:
        logs/
          2026-02-25/
            20260225T163401Z_orders_create_evt-001.json
            20260225T163802Z_inventory_levels_update_aa8bb3ad.json

    Each file contains:
        {
            "timestamp": "...",
            "received_at": "...",
            "status": "unread",
            "event_id": "...",
            "topic": "...",
            "shop_domain": "...",
            "payload": { ... }
        }
    """

    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

    def _build_filename(self, record: dict) -> str:
        """Build a unique, sortable filename from the record."""
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        topic = record.get("topic", "unknown").replace("/", "_")
        event_id = record.get("event_id", "no-id")
        # Truncate event_id for filename safety (UUIDs can be long)
        short_id = event_id[:12].replace("/", "-")
        return f"{ts}_{topic}_{short_id}.json"

    def _day_dir(self) -> str:
        """Return today's subdirectory, creating it if needed."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_path = os.path.join(self.log_dir, today)
        os.makedirs(day_path, exist_ok=True)
        return day_path

    def log(self, record: dict) -> str:
        """
        Write a single transaction to its own JSON file.
        Returns the path of the created file.
        """
        record["received_at"] = datetime.now(timezone.utc).isoformat()
        record.setdefault("status", "unread")

        day_dir = self._day_dir()
        filename = self._build_filename(record)
        file_path = os.path.join(day_dir, filename)
        lock_path = file_path + ".lock"

        content = json.dumps(record, ensure_ascii=False, indent=2) + "\n"

        lock = FileLock(lock_path, timeout=5)
        with lock:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())

        # Clean up lock file immediately
        try:
            os.remove(lock_path)
        except OSError:
            pass

        return file_path

    @staticmethod
    def update_status(file_path: str, new_status: str) -> dict:
        """
        Update the status of an existing transaction file.
        Returns the updated record.

        Usage:
            TransactionLogger.update_status("logs/2026-02-25/file.json", "read")
        """
        if new_status not in STATUSES:
            raise ValueError(f"Invalid status '{new_status}'. Must be one of: {STATUSES}")

        lock_path = file_path + ".lock"
        lock = FileLock(lock_path, timeout=5)
        with lock:
            with open(file_path, "r", encoding="utf-8") as f:
                record = json.load(f)

            record["status"] = new_status
            record["status_updated_at"] = datetime.now(timezone.utc).isoformat()

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())

        try:
            os.remove(lock_path)
        except OSError:
            pass

        return record

    def get_by_status(self, status: str, date: str = None) -> list[dict]:
        """
        Find all transactions with a given status.

        Args:
            status: "unread", "read", "processing", "processed", "error"
            date: Optional date string "YYYY-MM-DD" to limit search to one day.
                  If None, searches all dates.

        Returns:
            List of {"file": path, "record": dict} sorted oldest-first.
        """
        if status not in STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {STATUSES}")

        if date:
            pattern = os.path.join(self.log_dir, date, "*.json")
        else:
            pattern = os.path.join(self.log_dir, "*", "*.json")

        results = []
        for fpath in sorted(glob.glob(pattern)):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                if record.get("status") == status:
                    results.append({"file": fpath, "record": record})
            except (json.JSONDecodeError, OSError):
                continue

        return results
