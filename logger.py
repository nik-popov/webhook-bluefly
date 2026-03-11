import os
import json
import glob
import logging
from datetime import datetime, timezone

from filelock import FileLock

logger = logging.getLogger(__name__)

# Valid status transitions
STATUSES = ("unread", "read", "processing", "processed", "error")


def _is_d1_ref(ref: str) -> bool:
    return ref.startswith("d1:")


def _parse_d1_id(ref: str) -> int:
    return int(ref.split(":", 1)[1])


class TransactionLogger:
    """
    Logs each webhook event to Cloudflare D1 with filesystem fallback.

    D1 table: webhook_events
    Fallback: individual JSON files in logs/<date>/ directory.
    """

    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        self._d1 = None
        try:
            from d1_client import get_d1_client
            self._d1 = get_d1_client()
        except Exception:
            pass
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

    # ---- public API ----

    def log(self, record: dict) -> str:
        """
        Write a single transaction.
        Returns an identifier string (D1 ref or file path).
        """
        record["received_at"] = datetime.now(timezone.utc).isoformat()
        record.setdefault("status", "unread")

        if self._d1:
            try:
                return self._log_d1(record)
            except Exception as exc:
                logger.warning("D1 write failed, falling back to filesystem: %s", exc)

        return self._log_filesystem(record)

    def update_status(self, ref: str, new_status: str) -> dict:
        """
        Update the status of an existing transaction.
        Accepts a D1 ref ('d1:42') or file path.
        Returns the updated record.
        """
        if new_status not in STATUSES:
            raise ValueError(f"Invalid status '{new_status}'. Must be one of: {STATUSES}")

        if _is_d1_ref(ref):
            try:
                return self._update_status_d1(ref, new_status)
            except Exception as exc:
                logger.warning("D1 update_status failed: %s", exc)
                return {"status": new_status}

        return self._update_status_filesystem(ref, new_status)

    def get_by_status(self, status: str, date: str = None) -> list[dict]:
        """
        Find all transactions with a given status.
        Returns list of {"file": ref, "record": dict} sorted oldest-first.
        """
        if status not in STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {STATUSES}")

        if self._d1:
            try:
                return self._get_by_status_d1(status, date)
            except Exception as exc:
                logger.warning("D1 get_by_status failed, falling back to filesystem: %s", exc)

        return self._get_by_status_filesystem(status, date)

    def get_recent(self, limit: int = 200, date: str = None) -> list[dict]:
        """
        Return recent transactions (newest first), optionally filtered by date.
        Returns list of {"file": ref, "record": dict}.
        """
        if self._d1:
            try:
                return self._get_recent_d1(limit, date)
            except Exception as exc:
                logger.warning("D1 get_recent failed, falling back to filesystem: %s", exc)

        return self._get_recent_filesystem(limit, date)

    # ---- D1 implementations ----

    def _log_d1(self, record: dict) -> str:
        now = record["received_at"]
        created_date = now[:10]
        self._d1.execute(
            "INSERT INTO webhook_events "
            "(event_id, topic, shop_domain, status, timestamp, received_at, "
            "status_updated_at, payload, created_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                record.get("event_id", ""),
                record.get("topic", ""),
                record.get("shop_domain", ""),
                record.get("status", "unread"),
                record.get("timestamp", ""),
                now,
                None,
                json.dumps(record.get("payload", {})),
                created_date,
            ],
        )
        # Retrieve the inserted row id
        rows = self._d1.execute(
            "SELECT id FROM webhook_events WHERE event_id = ? AND received_at = ? "
            "ORDER BY id DESC LIMIT 1",
            [record.get("event_id", ""), now],
        )
        row_id = rows[0]["id"] if rows else 0
        return f"d1:{row_id}"

    def _update_status_d1(self, ref: str, new_status: str) -> dict:
        row_id = _parse_d1_id(ref)
        now = datetime.now(timezone.utc).isoformat()
        self._d1.execute(
            "UPDATE webhook_events SET status = ?, status_updated_at = ? WHERE id = ?",
            [new_status, now, row_id],
        )
        rows = self._d1.execute(
            "SELECT * FROM webhook_events WHERE id = ?", [row_id]
        )
        if rows:
            return self._row_to_record(rows[0])
        return {"status": new_status}

    def _get_by_status_d1(self, status: str, date: str = None) -> list[dict]:
        if date:
            rows = self._d1.execute(
                "SELECT * FROM webhook_events WHERE status = ? AND created_date = ? ORDER BY id ASC",
                [status, date],
            )
        else:
            rows = self._d1.execute(
                "SELECT * FROM webhook_events WHERE status = ? ORDER BY id ASC",
                [status],
            )
        return [{"file": f"d1:{r['id']}", "record": self._row_to_record(r)} for r in rows]

    def _get_recent_d1(self, limit: int, date: str = None) -> list[dict]:
        if date:
            rows = self._d1.execute(
                "SELECT * FROM webhook_events WHERE created_date = ? ORDER BY id DESC LIMIT ?",
                [date, limit],
            )
        else:
            rows = self._d1.execute(
                "SELECT * FROM webhook_events ORDER BY id DESC LIMIT ?",
                [limit],
            )
        return [{"file": f"d1:{r['id']}", "record": self._row_to_record(r)} for r in rows]

    @staticmethod
    def _row_to_record(row: dict) -> dict:
        """Convert a D1 row back to the record dict format."""
        rec = {
            "event_id": row.get("event_id", ""),
            "topic": row.get("topic", ""),
            "shop_domain": row.get("shop_domain", ""),
            "status": row.get("status", ""),
            "timestamp": row.get("timestamp", ""),
            "received_at": row.get("received_at", ""),
            "status_updated_at": row.get("status_updated_at"),
        }
        payload_raw = row.get("payload", "{}")
        try:
            rec["payload"] = json.loads(payload_raw) if payload_raw else {}
        except (json.JSONDecodeError, TypeError):
            rec["payload"] = {}
        return rec

    # ---- filesystem fallback implementations ----

    def _build_filename(self, record: dict) -> str:
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        topic = record.get("topic", "unknown").replace("/", "_")
        event_id = record.get("event_id", "no-id")
        short_id = event_id[:12].replace("/", "-")
        return f"{ts}_{topic}_{short_id}.json"

    def _day_dir(self) -> str:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_path = os.path.join(self.log_dir, today)
        os.makedirs(day_path, exist_ok=True)
        return day_path

    def _log_filesystem(self, record: dict) -> str:
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

        try:
            os.remove(lock_path)
        except OSError:
            pass

        return file_path

    @staticmethod
    def _update_status_filesystem(file_path: str, new_status: str) -> dict:
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

    def _get_by_status_filesystem(self, status: str, date: str = None) -> list[dict]:
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

    def _get_recent_filesystem(self, limit: int, date: str = None) -> list[dict]:
        if date:
            pattern = os.path.join(self.log_dir, date, "*.json")
        else:
            pattern = os.path.join(self.log_dir, "*", "*.json")

        results = []
        for fpath in sorted(glob.glob(pattern), reverse=True)[:limit]:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                results.append({"file": fpath, "record": record})
            except (json.JSONDecodeError, OSError):
                continue

        return results
