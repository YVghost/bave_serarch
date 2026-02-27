from __future__ import annotations

import os
import time
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, Optional
import requests
from pathlib import Path

SERPER_ENDPOINT = "https://google.serper.dev/search"
REQUESTS_LOG_FILE = "serper_keys_requests.json"

REQUEST_LIMIT = 1500         # límite seguro antes de 1000
GRACE_DAYS = 1               # días extra de seguridad
BASE_DELAY = 0.5             # espera base entre requests


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

    def increment_usage(self) -> bool:
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
        path = Path(self.usage_log_file)
        if path.exists():
            try:
                with open(path, "r") as f:
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
            if not self.usage[self.keys[i]].is_disabled():
                self.idx = i
                return
        raise RuntimeError("Todas las SERPER keys están deshabilitadas.")

    def current(self) -> str:
        return self.keys[self.idx]

    def rotate(self) -> bool:
        start = self.idx
        for _ in range(len(self.keys)):
            self.idx = (self.idx + 1) % len(self.keys)
            if not self.usage[self.current()].is_disabled():
                print(f"Rotando a key: {self.current()[:8]}...")
                return True
        self.idx = start
        return False

    def increment_current_key(self):
        key = self.current()
        disabled = self.usage[key].increment_usage()
        self._save_usage()

        if disabled:
            print(f"Key {key[:8]} alcanzó límite mensual. Deshabilitada.")
            self.rotate()

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
        key = key_manager.current()

        if key_manager.usage[key].is_disabled():
            if not key_manager.rotate():
                raise RuntimeError("No hay keys disponibles.")
            continue

        headers = {
            "X-API-KEY": key,
            "Content-Type": "application/json",
        }

        payload = {
            "q": query,
            "num": count,
        }

        try:
            time.sleep(BASE_DELAY)

            r = requests.post(
                SERPER_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=30
            )

            if r.status_code == 429:
                print("Rate limit detectado. Rotando key.")
                if not key_manager.rotate():
                    raise RuntimeError("Rate limit en todas las keys.")
                time.sleep(2 ** attempt)
                continue

            if r.status_code in (401, 403):
                print("Key inválida. Rotando.")
                if not key_manager.rotate():
                    raise RuntimeError("Todas las keys inválidas.")
                continue

            if 500 <= r.status_code < 600:
                time.sleep(2 ** attempt)
                continue

            r.raise_for_status()
            data = r.json()

            key_manager.increment_current_key()

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

    raise RuntimeError(
        f"Serper search falló tras {max_retries} reintentos. Último error: {last_err}"
    )