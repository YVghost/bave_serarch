from __future__ import annotations

import os
import time
import random
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, Optional
import requests
from pathlib import Path

SERPER_ENDPOINT = "https://google.serper.dev/search"
REQUESTS_LOG_FILE = "serper_keys_requests.json"
REQUEST_LIMIT = 980
GRACE_DAYS = 1


@dataclass
class KeyStatus:
    ok: bool
    reason: str
    status_code: int | None = None


@dataclass
class KeyUsage:
    key: str
    request_count: int
    last_request_date: str
    disabled_until: Optional[str] = None

    def is_disabled(self) -> bool:
        if not self.disabled_until:
            return False
        return datetime.now() < datetime.fromisoformat(self.disabled_until)

    def increment_usage(self):
        self.request_count += 1
        self.last_request_date = datetime.now().isoformat()

        if self.request_count >= REQUEST_LIMIT:
            disable_period = timedelta(days=30 + GRACE_DAYS)
            self.disabled_until = (datetime.now() + disable_period).isoformat()
            return True
        return False


class SerperKeyManager:
    def __init__(self, keys: list[str], usage_log_file: str = REQUESTS_LOG_FILE):
        self.keys = [k.strip() for k in keys if k and k.strip()]
        if not self.keys:
            raise RuntimeError("No se encontraron SERPER_API_KEYS")

        self.usage_log_file = usage_log_file
        self.usage: Dict[str, KeyUsage] = {}
        self.idx = 0

        self._load_usage()
        self._initialize_new_keys()
        self._find_first_valid_key()

    def _load_usage(self):
        log_path = Path(self.usage_log_file)
        if log_path.exists():
            try:
                with open(log_path, "r") as f:
                    data = json.load(f)
                    for key, usage_data in data.items():
                        self.usage[key] = KeyUsage(**usage_data)
            except Exception:
                pass

    def _save_usage(self):
        try:
            data = {key: asdict(usage) for key, usage in self.usage.items()}
            with open(self.usage_log_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    def _initialize_new_keys(self):
        for key in self.keys:
            if key not in self.usage:
                self.usage[key] = KeyUsage(
                    key=key,
                    request_count=0,
                    last_request_date=datetime.now().isoformat(),
                )
        self._save_usage()

    def _find_first_valid_key(self):
        for i in range(len(self.keys)):
            key = self.keys[i]
            if not self.usage[key].is_disabled():
                self.idx = i
                return
        raise RuntimeError("Todas las SERPER keys están deshabilitadas.")

    def current(self) -> str:
        return self.keys[self.idx]

    def _increment_current_key_usage(self):
        key = self.current()
        disabled = self.usage[key].increment_usage()
        self._save_usage()
        if disabled:
            print(f"Key {key[:8]}... alcanzó {REQUEST_LIMIT} requests.")
            self.rotate()

    def rotate(self) -> bool:
        start = self.idx
        for _ in range(len(self.keys)):
            self.idx = (self.idx + 1) % len(self.keys)
            if not self.usage[self.current()].is_disabled():
                return True
        self.idx = start
        return False

    def check_key(self, key: str, timeout: int = 15) -> KeyStatus:
        headers = {
            "X-API-KEY": key,
            "Content-Type": "application/json",
        }
        payload = {"q": "test", "num": 1}

        try:
            r = requests.post(SERPER_ENDPOINT, headers=headers, json=payload, timeout=timeout)

            if r.status_code == 200:
                return KeyStatus(True, "OK", 200)
            if r.status_code in (401, 403):
                return KeyStatus(False, "UNAUTHORIZED_OR_FORBIDDEN", r.status_code)
            if r.status_code == 429:
                return KeyStatus(False, "RATE_LIMIT", 429)
            if 500 <= r.status_code < 600:
                return KeyStatus(False, "SERVER_ERROR", r.status_code)
            return KeyStatus(False, "OTHER_ERROR", r.status_code)

        except requests.RequestException:
            return KeyStatus(False, "NETWORK_ERROR", None)

    def ensure_working_key(self) -> str:
        rotations = 0
        while rotations < len(self.keys):
            key = self.current()

            if self.usage[key].is_disabled():
                if not self.rotate():
                    break
                rotations += 1
                continue

            status = self.check_key(key)
            if status.ok:
                return key

            if status.reason in ("RATE_LIMIT", "UNAUTHORIZED_OR_FORBIDDEN"):
                if not self.rotate():
                    break
                rotations += 1
                continue

            if status.reason in ("SERVER_ERROR", "NETWORK_ERROR"):
                time.sleep(1)
                continue

            if not self.rotate():
                break
            rotations += 1

        raise RuntimeError("No se encontró SERPER API key funcional.")

    @staticmethod
    def from_env() -> "SerperKeyManager":
        keys_csv = os.getenv("SERPER_API_KEYS", "").strip()
        keys = []

        if keys_csv:
            keys = [k.strip() for k in keys_csv.split(",") if k.strip()]

        if not keys:
            i = 1
            while True:
                k = os.getenv(f"SERPER_API_KEY_{i}", "").strip()
                if not k:
                    break
                keys.append(k)
                i += 1

        if not keys:
            k = os.getenv("SERPER_API_KEY", "").strip()
            if k:
                keys = [k]

        return SerperKeyManager(keys)


def serper_search_with_rotation(
    key_manager: SerperKeyManager,
    query: str,
    count: int = 8,
    max_retries: int = 5,
) -> list[dict]:

    last_err = None

    for attempt in range(max_retries):
        key = key_manager.ensure_working_key()

        headers = {
            "X-API-KEY": key,
            "Content-Type": "application/json",
        }

        payload = {
            "q": query,
            "num": count,
        }

        try:
            r = requests.post(SERPER_ENDPOINT, headers=headers, json=payload, timeout=30)

            if r.status_code == 429:
                if not key_manager.rotate():
                    raise RuntimeError("Rate limit en todas las keys.")
                time.sleep(2 ** attempt)
                continue

            if r.status_code in (401, 403):
                if not key_manager.rotate():
                    raise RuntimeError("Todas las keys inválidas.")
                continue

            if 500 <= r.status_code < 600:
                time.sleep(2 ** attempt)
                continue

            r.raise_for_status()
            data = r.json()

            key_manager._increment_current_key_usage()

            results = []
            for item in data.get("organic", []):
                results.append(
                    {
                        "url": item.get("link", ""),
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                    }
                )

            return results

        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)

    raise RuntimeError(f"Serper search falló tras {max_retries} reintentos. Último error: {last_err}")