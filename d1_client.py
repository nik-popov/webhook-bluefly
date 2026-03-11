"""Cloudflare D1 database client for configuration and log storage."""

import json
import logging
import os
import threading
import time

import requests

logger = logging.getLogger(__name__)

_CONFIG_FALLBACK_PATH = os.path.join(os.path.dirname(__file__), "config.json")


class D1Client:
    """Thin wrapper around the Cloudflare D1 REST API."""

    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.url = (
            f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
            f"/d1/database/{database_id}/query"
        )
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

    def execute(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute a single SQL statement and return result rows."""
        body = {"sql": sql}
        if params:
            body["params"] = params
        resp = requests.post(self.url, headers=self.headers, json=body, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            errors = data.get("errors", [])
            raise RuntimeError(f"D1 query failed: {errors}")
        results = data.get("result", [])
        if results and "results" in results[0]:
            return results[0]["results"]
        return []

    def load_all(self) -> dict:
        """Load all config rows as a dict."""
        rows = self.execute("SELECT key, value FROM config")
        cfg = {}
        for row in rows:
            raw = row["value"]
            try:
                cfg[row["key"]] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                cfg[row["key"]] = raw
        return cfg

    def save_all(self, cfg: dict) -> None:
        """Save all config keys via INSERT OR REPLACE."""
        for key, value in cfg.items():
            serialized = json.dumps(value) if not isinstance(value, str) else value
            self.execute(
                "INSERT OR REPLACE INTO config (key, value, updated_at) "
                "VALUES (?, ?, datetime('now'))",
                [key, serialized],
            )


class CachedD1Config:
    """In-memory cache over D1 with local file fallback."""

    def __init__(self, d1_client: D1Client, ttl_seconds: int = 30):
        self._d1 = d1_client
        self._ttl = ttl_seconds
        self._cache: dict | None = None
        self._cache_time: float = 0
        self._lock = threading.Lock()

    def load(self) -> dict:
        now = time.time()
        if self._cache is not None and (now - self._cache_time) < self._ttl:
            return self._cache.copy()
        with self._lock:
            if self._cache is not None and (time.time() - self._cache_time) < self._ttl:
                return self._cache.copy()
            try:
                cfg = self._d1.load_all()
            except Exception as exc:
                logger.warning("D1 unreachable, falling back to config.json: %s", exc)
                cfg = self._load_fallback()
            self._cache = cfg
            self._cache_time = time.time()
            return cfg.copy()

    def save(self, cfg: dict) -> None:
        try:
            self._d1.save_all(cfg)
        except Exception as exc:
            logger.warning("D1 unreachable, falling back to config.json for save: %s", exc)
            self._save_fallback(cfg)
        with self._lock:
            self._cache = cfg.copy()
            self._cache_time = time.time()

    @staticmethod
    def _load_fallback() -> dict:
        try:
            with open(_CONFIG_FALLBACK_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _save_fallback(cfg: dict) -> None:
        with open(_CONFIG_FALLBACK_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
            f.write("\n")


_store: CachedD1Config | None = None
_store_lock = threading.Lock()

_raw_client: D1Client | None = None
_raw_lock = threading.Lock()


def _build_d1_client() -> D1Client:
    """Create a D1Client from environment variables."""
    account_id = os.environ.get("D1_ACCOUNT_ID", "")
    database_id = os.environ.get("D1_DATABASE_ID", "")
    api_token = os.environ.get("D1_API_TOKEN", "")
    return D1Client(account_id, database_id, api_token)


def get_config_store() -> CachedD1Config:
    """Return the module-level CachedD1Config singleton (lazy init from env vars)."""
    global _store
    if _store is not None:
        return _store
    with _store_lock:
        if _store is not None:
            return _store
        _store = CachedD1Config(_build_d1_client())
        return _store


def get_d1_client() -> D1Client:
    """Return a raw D1Client singleton for non-config tables (logs, pipeline)."""
    global _raw_client
    if _raw_client is not None:
        return _raw_client
    with _raw_lock:
        if _raw_client is not None:
            return _raw_client
        _raw_client = _build_d1_client()
        return _raw_client
