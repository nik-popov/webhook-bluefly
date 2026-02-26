"""
Internal pipeline transaction log.

Tracks each product through the processing pipeline:
  queued → enriching → enriched → mapping → mapped → pushing → pushed | error | skipped

Separate from the webhook logger (logger.py) — this tracks processing state,
not raw Shopify events.

Directory layout:
    pipeline_logs/
      2026-02-25/
        20260225T163401Z_product_9647282618663.json
"""

import os
import json
import glob
from datetime import datetime, timezone

from filelock import FileLock


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
)


class PipelineLogger:
    def __init__(self, log_dir: str = "./pipeline_logs"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

    def _day_dir(self) -> str:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_path = os.path.join(self.log_dir, today)
        os.makedirs(day_path, exist_ok=True)
        return day_path

    def create_job(
        self,
        source_file: str,
        topic: str,
        product_id,
        event_id: str,
    ) -> str:
        """
        Create a new pipeline job file.

        Returns the job file path.
        """
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

    def update_stage(
        self,
        job_path: str,
        stage: str,
        data: dict = None,
        error: str = None,
    ) -> dict:
        """
        Update pipeline job to a new stage.

        Appends to stages[] history and updates top-level status.
        Returns the updated record.
        """
        if stage not in PIPELINE_STATUSES:
            raise ValueError(
                f"Invalid stage '{stage}'. Must be one of: {PIPELINE_STATUSES}"
            )

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

    def get_jobs_by_status(self, status: str, date: str = None) -> list[dict]:
        """
        Find all pipeline jobs with a given status.

        Args:
            status: One of PIPELINE_STATUSES
            date: Optional "YYYY-MM-DD" to limit search

        Returns:
            List of {"file": path, "record": dict} sorted oldest-first.
        """
        if status not in PIPELINE_STATUSES:
            raise ValueError(
                f"Invalid status '{status}'. Must be one of: {PIPELINE_STATUSES}"
            )

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
