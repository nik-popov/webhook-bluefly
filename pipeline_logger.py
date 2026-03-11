"""
Internal pipeline transaction log.

Tracks each product through the processing pipeline:
  queued -> enriching -> enriched -> mapping -> mapped -> pushing -> pushed | error | skipped

Primary storage: Cloudflare D1 (tables: pipeline_jobs, pipeline_errors)
Fallback: individual JSON files in pipeline_logs/<date>/ directory.
"""

import os
import json
import glob
import logging
from datetime import datetime, timezone

from filelock import FileLock

logger = logging.getLogger(__name__)

PIPELINE_STATUSES = (
    "queued",
    "enriching",
    "enriched",
    "mapping",
    "mapped",
    "pushing",
    "pushed",
    "error",
    "skipped",
    "size_error",
)

ERROR_LOG_DIR = "./pipeline_logs_errors"


def _is_d1_ref(ref: str) -> bool:
    return isinstance(ref, str) and ref.startswith("d1:")


def _parse_d1_id(ref: str) -> int:
    return int(ref.split(":", 1)[1])


class PipelineLogger:
    def __init__(self, log_dir: str = "./pipeline_logs"):
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

    def create_job(
        self,
        source_file: str,
        topic: str,
        product_id,
        event_id: str,
    ) -> str:
        """Create a new pipeline job. Returns an identifier (D1 ref or file path)."""
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        job_id = f"{ts}_product_{product_id}"

        record = {
            "job_id": job_id,
            "source_webhook_file": source_file,
            "topic": topic,
            "product_id": product_id,
            "event_id": event_id,
            "created_at": now.isoformat(),
            "status": "queued",
            "error": None,
            "stages": [
                {
                    "stage": "queued",
                    "timestamp": now.isoformat(),
                    "data": None,
                    "error": None,
                }
            ],
        }

        if self._d1:
            try:
                return self._create_job_d1(record, now)
            except Exception as exc:
                logger.warning("D1 create_job failed, falling back to filesystem: %s", exc)

        return self._create_job_filesystem(record, job_id)

    def update_stage(
        self,
        job_ref: str,
        stage: str,
        data: dict = None,
        error: str = None,
    ) -> dict:
        """Update pipeline job to a new stage. Returns the updated record."""
        if stage not in PIPELINE_STATUSES:
            raise ValueError(
                f"Invalid stage '{stage}'. Must be one of: {PIPELINE_STATUSES}"
            )

        if _is_d1_ref(job_ref):
            try:
                return self._update_stage_d1(job_ref, stage, data, error)
            except Exception as exc:
                logger.warning("D1 update_stage failed: %s", exc)
                return {"status": stage}

        return self._update_stage_filesystem(job_ref, stage, data, error)

    def patch_product_id(self, job_ref: str, product_id) -> None:
        """Overwrite the top-level product_id once the real Shopify ID is known."""
        if _is_d1_ref(job_ref):
            try:
                row_id = _parse_d1_id(job_ref)
                self._d1.execute(
                    "UPDATE pipeline_jobs SET product_id = ? WHERE id = ?",
                    [str(product_id), row_id],
                )
                return
            except Exception as exc:
                logger.warning("D1 patch_product_id failed: %s", exc)
                return

        self._patch_product_id_filesystem(job_ref, product_id)

    def create_error_job(
        self,
        product_id,
        title: str,
        category_id: str,
        size_errors: list[dict],
        source: str = "dashboard",
    ) -> str:
        """Write a product with unmappable sizes. Returns identifier."""
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        job_id = f"{ts}_product_{product_id}"

        if self._d1:
            try:
                created_date = now.strftime("%Y-%m-%d")
                self._d1.execute(
                    "INSERT INTO pipeline_errors "
                    "(job_id, product_id, title, category_id, source, "
                    "created_at, status, size_errors, created_date) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        job_id,
                        str(product_id),
                        title,
                        category_id,
                        source,
                        now.isoformat(),
                        "size_error",
                        json.dumps(size_errors),
                        created_date,
                    ],
                )
                rows = self._d1.execute(
                    "SELECT id FROM pipeline_errors WHERE job_id = ? ORDER BY id DESC LIMIT 1",
                    [job_id],
                )
                row_id = rows[0]["id"] if rows else 0
                return f"d1:{row_id}"
            except Exception as exc:
                logger.warning("D1 create_error_job failed, falling back to filesystem: %s", exc)

        return self._create_error_job_filesystem(
            product_id, title, category_id, size_errors, source, now, job_id
        )

    def get_error_jobs(self) -> list[dict]:
        """Return all size-error jobs, newest first."""
        if self._d1:
            try:
                rows = self._d1.execute(
                    "SELECT * FROM pipeline_errors ORDER BY id DESC"
                )
                results = []
                for r in rows:
                    rec = {
                        "job_id": r.get("job_id", ""),
                        "product_id": r.get("product_id", ""),
                        "title": r.get("title", ""),
                        "category_id": r.get("category_id", ""),
                        "source": r.get("source", "dashboard"),
                        "created_at": r.get("created_at", ""),
                        "status": r.get("status", "size_error"),
                        "_file": f"d1:{r['id']}",
                    }
                    try:
                        rec["size_errors"] = json.loads(r.get("size_errors", "[]"))
                    except (json.JSONDecodeError, TypeError):
                        rec["size_errors"] = []
                    results.append(rec)
                return results
            except Exception as exc:
                logger.warning("D1 get_error_jobs failed, falling back to filesystem: %s", exc)

        return self._get_error_jobs_filesystem()

    def delete_error_job(self, ref: str) -> bool:
        """Remove a resolved error job."""
        if _is_d1_ref(ref):
            try:
                row_id = _parse_d1_id(ref)
                self._d1.execute("DELETE FROM pipeline_errors WHERE id = ?", [row_id])
                return True
            except Exception as exc:
                logger.warning("D1 delete_error_job failed: %s", exc)
                return False

        try:
            os.remove(ref)
            return True
        except OSError:
            return False

    def get_jobs_by_status(self, status: str, date: str = None) -> list[dict]:
        """Find all pipeline jobs with a given status."""
        if status not in PIPELINE_STATUSES:
            raise ValueError(
                f"Invalid status '{status}'. Must be one of: {PIPELINE_STATUSES}"
            )

        if self._d1:
            try:
                return self._get_jobs_by_status_d1(status, date)
            except Exception as exc:
                logger.warning("D1 get_jobs_by_status failed, falling back to filesystem: %s", exc)

        return self._get_jobs_by_status_filesystem(status, date)

    def get_recent_jobs(self, limit: int = 200, date: str = None) -> list[dict]:
        """Return recent pipeline jobs (newest first)."""
        if self._d1:
            try:
                if date:
                    rows = self._d1.execute(
                        "SELECT * FROM pipeline_jobs WHERE created_date = ? ORDER BY id DESC LIMIT ?",
                        [date, limit],
                    )
                else:
                    rows = self._d1.execute(
                        "SELECT * FROM pipeline_jobs ORDER BY id DESC LIMIT ?",
                        [limit],
                    )
                return [
                    {"file": f"d1:{r['id']}", "record": self._row_to_record(r)}
                    for r in rows
                ]
            except Exception as exc:
                logger.warning("D1 get_recent_jobs failed, falling back to filesystem: %s", exc)

        return self._get_recent_jobs_filesystem(limit, date)

    def get_synced_product_ids(self) -> dict:
        """Return {product_id: {status, time, size_errors?}} for all jobs.

        Replaces the filesystem-based _get_synced_product_ids() in routes.py.
        """
        if self._d1:
            try:
                rows = self._d1.execute(
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
                    # For inventory webhooks the real product_id is in enriched stage
                    for stage in stages:
                        if stage.get("stage") == "enriched":
                            enriched_pid = (stage.get("data") or {}).get("product_id")
                            if enriched_pid:
                                pid = enriched_pid
                            break
                    if pid:
                        pid = str(pid)
                        entry = {
                            "status": row.get("status", "unknown"),
                            "time": row.get("created_at"),
                        }
                        for stage in stages:
                            if stage.get("stage") in ("pushed", "size_error"):
                                se = (stage.get("data") or {}).get("size_errors")
                                if se:
                                    entry["size_errors"] = se
                                break
                        result[pid] = entry
                return result
            except Exception as exc:
                logger.warning("D1 get_synced_product_ids failed, falling back to filesystem: %s", exc)

        return self._get_synced_product_ids_filesystem()

    def reset_all_jobs(self) -> int:
        """Delete ALL pipeline jobs. Returns count deleted."""
        if self._d1:
            try:
                rows = self._d1.execute("SELECT COUNT(*) as cnt FROM pipeline_jobs")
                count = rows[0]["cnt"] if rows else 0
                self._d1.execute("DELETE FROM pipeline_jobs")
                return count
            except Exception as exc:
                logger.warning("D1 reset_all_jobs failed, falling back to filesystem: %s", exc)

        pattern = os.path.join(self.log_dir, "*", "*.json")
        count = 0
        for fpath in glob.glob(pattern):
            try:
                os.remove(fpath)
                count += 1
            except OSError:
                continue
        return count

    def delete_jobs_by_product_ids(self, product_ids: set[str]) -> tuple[int, set[str]]:
        """Delete pipeline jobs matching given product IDs.

        Returns (deleted_count, pushed_skus) — SKUs extracted from pushed stages
        before deletion, for portal cleanup.
        """
        if self._d1:
            try:
                return self._delete_jobs_by_product_ids_d1(product_ids)
            except Exception as exc:
                logger.warning("D1 delete_jobs_by_product_ids failed, falling back to filesystem: %s", exc)

        return self._delete_jobs_by_product_ids_filesystem(product_ids)

    # ---- D1 implementations ----

    def _create_job_d1(self, record: dict, now: datetime) -> str:
        created_date = now.strftime("%Y-%m-%d")
        self._d1.execute(
            "INSERT INTO pipeline_jobs "
            "(job_id, source_webhook_file, topic, product_id, event_id, "
            "created_at, status, error, stages, created_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                record["job_id"],
                record["source_webhook_file"],
                record["topic"],
                str(record["product_id"]),
                record["event_id"],
                record["created_at"],
                record["status"],
                record.get("error"),
                json.dumps(record["stages"]),
                created_date,
            ],
        )
        rows = self._d1.execute(
            "SELECT id FROM pipeline_jobs WHERE job_id = ? ORDER BY id DESC LIMIT 1",
            [record["job_id"]],
        )
        row_id = rows[0]["id"] if rows else 0
        return f"d1:{row_id}"

    def _update_stage_d1(self, ref: str, stage: str, data: dict = None, error: str = None) -> dict:
        row_id = _parse_d1_id(ref)
        rows = self._d1.execute(
            "SELECT stages, status FROM pipeline_jobs WHERE id = ?", [row_id]
        )
        if not rows:
            return {"status": stage}

        current_stages = []
        try:
            current_stages = json.loads(rows[0].get("stages", "[]") or "[]")
        except (json.JSONDecodeError, TypeError):
            pass

        now = datetime.now(timezone.utc).isoformat()
        current_stages.append({
            "stage": stage,
            "timestamp": now,
            "data": data,
            "error": error,
        })

        update_error = error if error else rows[0].get("error")
        self._d1.execute(
            "UPDATE pipeline_jobs SET status = ?, error = ?, stages = ? WHERE id = ?",
            [stage, update_error, json.dumps(current_stages), row_id],
        )

        return {
            "status": stage,
            "stages": current_stages,
            "error": update_error,
        }

    def _get_jobs_by_status_d1(self, status: str, date: str = None) -> list[dict]:
        if date:
            rows = self._d1.execute(
                "SELECT * FROM pipeline_jobs WHERE status = ? AND created_date = ? ORDER BY id ASC",
                [status, date],
            )
        else:
            rows = self._d1.execute(
                "SELECT * FROM pipeline_jobs WHERE status = ? ORDER BY id ASC",
                [status],
            )
        return [{"file": f"d1:{r['id']}", "record": self._row_to_record(r)} for r in rows]

    def _delete_jobs_by_product_ids_d1(self, product_ids: set[str]) -> tuple[int, set[str]]:
        """D1 implementation: delete jobs matching product_ids, return (count, skus)."""
        # Fetch all jobs to check enriched-stage product_id and extract SKUs
        rows = self._d1.execute(
            "SELECT id, product_id, stages FROM pipeline_jobs ORDER BY id ASC"
        )
        ids_to_delete = []
        pushed_skus = set()
        for row in rows:
            pid = str(row.get("product_id", ""))
            stages = []
            try:
                stages = json.loads(row.get("stages", "[]") or "[]")
            except (json.JSONDecodeError, TypeError):
                pass
            for stage in stages:
                if stage.get("stage") == "enriched":
                    ep = str((stage.get("data") or {}).get("product_id", ""))
                    if ep:
                        pid = ep
                    break
            if pid in product_ids:
                for stage in stages:
                    if stage.get("stage") in ("pushed", "size_error"):
                        sku = (stage.get("data") or {}).get("sku", "")
                        if sku:
                            pushed_skus.add(sku)
                        break
                ids_to_delete.append(row["id"])

        for row_id in ids_to_delete:
            self._d1.execute("DELETE FROM pipeline_jobs WHERE id = ?", [row_id])

        return len(ids_to_delete), pushed_skus

    @staticmethod
    def _row_to_record(row: dict) -> dict:
        """Convert a D1 row to the record dict format."""
        rec = {
            "job_id": row.get("job_id", ""),
            "source_webhook_file": row.get("source_webhook_file", ""),
            "topic": row.get("topic", ""),
            "product_id": row.get("product_id", ""),
            "event_id": row.get("event_id", ""),
            "created_at": row.get("created_at", ""),
            "status": row.get("status", ""),
            "error": row.get("error"),
        }
        try:
            rec["stages"] = json.loads(row.get("stages", "[]") or "[]")
        except (json.JSONDecodeError, TypeError):
            rec["stages"] = []
        return rec

    # ---- filesystem fallback implementations ----

    def _day_dir(self) -> str:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_path = os.path.join(self.log_dir, today)
        os.makedirs(day_path, exist_ok=True)
        return day_path

    def _create_job_filesystem(self, record: dict, job_id: str) -> str:
        day_dir = self._day_dir()
        filename = f"{job_id}.json"
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
    def _update_stage_filesystem(job_path: str, stage: str, data: dict = None, error: str = None) -> dict:
        lock_path = job_path + ".lock"
        lock = FileLock(lock_path, timeout=5)

        with lock:
            with open(job_path, "r", encoding="utf-8") as f:
                record = json.load(f)

            now = datetime.now(timezone.utc).isoformat()
            record["status"] = stage
            record["stages"].append(
                {
                    "stage": stage,
                    "timestamp": now,
                    "data": data,
                    "error": error,
                }
            )
            if error:
                record["error"] = error

            with open(job_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())

        try:
            os.remove(lock_path)
        except OSError:
            pass

        return record

    @staticmethod
    def _patch_product_id_filesystem(job_path: str, product_id) -> None:
        lock_path = job_path + ".lock"
        lock = FileLock(lock_path, timeout=5)
        with lock:
            with open(job_path, "r", encoding="utf-8") as f:
                record = json.load(f)
            record["product_id"] = product_id
            with open(job_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        try:
            os.remove(lock_path)
        except OSError:
            pass

    def _create_error_job_filesystem(
        self, product_id, title, category_id, size_errors, source, now, job_id
    ) -> str:
        error_dir = os.path.join(
            os.path.dirname(self.log_dir), "pipeline_logs_errors"
        )
        today = now.strftime("%Y-%m-%d")
        day_dir = os.path.join(error_dir, today)
        os.makedirs(day_dir, exist_ok=True)

        record = {
            "job_id": job_id,
            "product_id": product_id,
            "title": title,
            "category_id": category_id,
            "source": source,
            "created_at": now.isoformat(),
            "status": "size_error",
            "size_errors": size_errors,
        }

        file_path = os.path.join(day_dir, f"{job_id}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        return file_path

    def _get_error_jobs_filesystem(self) -> list[dict]:
        error_dir = os.path.join(
            os.path.dirname(self.log_dir), "pipeline_logs_errors"
        )
        pattern = os.path.join(error_dir, "*", "*.json")
        results = []
        for fpath in sorted(glob.glob(pattern), reverse=True):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                record["_file"] = fpath
                results.append(record)
            except (json.JSONDecodeError, OSError):
                continue
        return results

    def _get_jobs_by_status_filesystem(self, status: str, date: str = None) -> list[dict]:
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

    def _get_recent_jobs_filesystem(self, limit: int, date: str = None) -> list[dict]:
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

    def _delete_jobs_by_product_ids_filesystem(self, product_ids: set[str]) -> tuple[int, set[str]]:
        pattern = os.path.join(self.log_dir, "*", "*.json")
        deleted = 0
        pushed_skus = set()
        for fpath in glob.glob(pattern):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                pid = str(record.get("product_id", ""))
                for stage in record.get("stages", []):
                    if stage.get("stage") == "enriched":
                        ep = str((stage.get("data") or {}).get("product_id", ""))
                        if ep:
                            pid = ep
                        break
                if pid in product_ids:
                    for stage in record.get("stages", []):
                        if stage.get("stage") in ("pushed", "size_error"):
                            sku = (stage.get("data") or {}).get("sku", "")
                            if sku:
                                pushed_skus.add(sku)
                            break
                    os.remove(fpath)
                    deleted += 1
            except (OSError, json.JSONDecodeError):
                continue
        return deleted, pushed_skus

    def _get_synced_product_ids_filesystem(self) -> dict:
        result = {}
        pattern = os.path.join(self.log_dir, "*", "*.json")
        for fpath in sorted(glob.glob(pattern), key=os.path.getmtime):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                pid = record.get("product_id")
                for stage in record.get("stages", []):
                    if stage.get("stage") == "enriched":
                        enriched_pid = (stage.get("data") or {}).get("product_id")
                        if enriched_pid:
                            pid = enriched_pid
                        break
                if pid:
                    pid = str(pid)
                    entry = {
                        "status": record.get("status", "unknown"),
                        "time": record.get("created_at"),
                    }
                    for stage in record.get("stages", []):
                        if stage.get("stage") in ("pushed", "size_error"):
                            se = (stage.get("data") or {}).get("size_errors")
                            if se:
                                entry["size_errors"] = se
                            break
                    result[pid] = entry
            except (json.JSONDecodeError, OSError):
                continue
        return result
