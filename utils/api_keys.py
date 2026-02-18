# utils/api_keys.py
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

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
REQUESTS_LOG_FILE = "brave_keys_requests.json"
REQUEST_LIMIT = 980
GRACE_DAYS = 1  # Días de gracia después del mes

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
        """Verifica si la key está deshabilitada y si ya pasó el período de deshabilitación"""
        if not self.disabled_until:
            return False
        
        disabled_until = datetime.fromisoformat(self.disabled_until)
        return datetime.now() < disabled_until
    
    def increment_usage(self):
        """Incrementa el contador de uso y verifica si debe deshabilitarse"""
        self.request_count += 1
        self.last_request_date = datetime.now().isoformat()
        
        # Si alcanza el límite, deshabilitar por un mes + 1 día
        if self.request_count >= REQUEST_LIMIT:
            disable_period = timedelta(days=30 + GRACE_DAYS)
            self.disabled_until = (datetime.now() + disable_period).isoformat()
            return True  # Indica que se deshabilitó
        return False

class BraveKeyManager:
    """
    Administra múltiples API keys.
    - Lleva registro de requests por key.
    - Deshabilita keys que alcanzan 980 requests por un mes + 1 día.
    - Si una key se rate-limitea (429) o falla (401/403), rota a la siguiente.
    - Si hay 5xx o network error, reintenta sin rotar inmediatamente.
    """
    def __init__(self, keys: list[str], usage_log_file: str = REQUESTS_LOG_FILE):
        self.keys = [k.strip() for k in keys if k and k.strip()]
        if not self.keys:
            raise RuntimeError("No se encontraron Brave API Keys en .env")
        
        self.usage_log_file = usage_log_file
        self.usage: Dict[str, KeyUsage] = {}
        self.idx = 0
        
        # Cargar registro de uso existente
        self._load_usage()
        
        # Inicializar registro para keys nuevas
        self._initialize_new_keys()
        
        # Encontrar el primer índice con key no deshabilitada
        self._find_first_valid_key()

    def _load_usage(self):
        """Carga el registro de uso desde el archivo"""
        log_path = Path(self.usage_log_file)
        if log_path.exists():
            try:
                with open(log_path, 'r') as f:
                    data = json.load(f)
                    for key, usage_data in data.items():
                        self.usage[key] = KeyUsage(**usage_data)
            except (json.JSONDecodeError, IOError):
                # Si hay error, empezar con registro vacío
                pass
    
    def _save_usage(self):
        """Guarda el registro de uso en el archivo"""
        try:
            data = {key: asdict(usage) for key, usage in self.usage.items()}
            with open(self.usage_log_file, 'w') as f:
                json.dump(data, f, indent=2)
        except IOError:
            # Si no se puede guardar, al menos continuar
            pass
    
    def _initialize_new_keys(self):
        """Inicializa el registro para keys que no existen en el registro"""
        for key in self.keys:
            if key not in self.usage:
                self.usage[key] = KeyUsage(
                    key=key,
                    request_count=0,
                    last_request_date=datetime.now().isoformat()
                )
        self._save_usage()
    
    def _find_first_valid_key(self):
        """Encuentra el primer índice con una key válida (no deshabilitada)"""
        start_idx = self.idx
        for i in range(len(self.keys)):
            idx = (start_idx + i) % len(self.keys)
            key = self.keys[idx]
            usage = self.usage.get(key)
            
            if usage and not usage.is_disabled():
                self.idx = idx
                return
        
        # Si todas están deshabilitadas, lanzar error
        disabled_until = self.usage.get(self.keys[0], KeyUsage(self.keys[0], 0, "")).disabled_until
        raise RuntimeError(f"Todas las keys están deshabilitadas hasta {disabled_until}")

    def current(self) -> str:
        return self.keys[self.idx]
    
    def _increment_current_key_usage(self) -> bool:
        """Incrementa el uso de la key actual y verifica si se deshabilitó"""
        key = self.current()
        usage = self.usage[key]
        disabled = usage.increment_usage()
        self._save_usage()
        
        if disabled:
            print(f"Key {key[:8]}... alcanzó {REQUEST_LIMIT} requests. Deshabilitada por 30+{GRACE_DAYS} días.")
            # Buscar siguiente key válida
            self._find_first_valid_key()
        
        return disabled

    def rotate(self) -> bool:
        """Pasa a la siguiente key. Devuelve False si ya no hay más."""
        start_idx = self.idx
        attempts = 0
        
        while attempts < len(self.keys):
            self.idx = (self.idx + 1) % len(self.keys)
            key = self.current()
            usage = self.usage.get(key)
            
            # Si la key no está deshabilitada, usarla
            if usage and not usage.is_disabled():
                return True
            
            attempts += 1
        
        # Si llegamos aquí, todas están deshabilitadas
        self.idx = start_idx  # Restaurar índice original
        return False

    def check_key(self, key: str, timeout: int = 15) -> KeyStatus:
        """
        Chequeo liviano: realiza una búsqueda mínima.
        """
        headers = {"Accept": "application/json", "X-Subscription-Token": key}
        params = {"q": "site:linkedin.com/in test", "count": 1}
        try:
            r = requests.get(BRAVE_ENDPOINT, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return KeyStatus(ok=True, reason="OK", status_code=200)
            if r.status_code in (401, 403):
                return KeyStatus(ok=False, reason="UNAUTHORIZED_OR_FORBIDDEN", status_code=r.status_code)
            if r.status_code == 429:
                return KeyStatus(ok=False, reason="RATE_LIMIT", status_code=429)
            if 500 <= r.status_code < 600:
                return KeyStatus(ok=False, reason="SERVER_ERROR", status_code=r.status_code)
            return KeyStatus(ok=False, reason="OTHER_ERROR", status_code=r.status_code)
        except requests.RequestException:
            return KeyStatus(ok=False, reason="NETWORK_ERROR", status_code=None)

    def ensure_working_key(self, max_rotations: int | None = None) -> str:
        """
        Garantiza una key funcional. Si la actual no sirve, rota hasta encontrar una OK.
        """
        rotations = 0
        while True:
            key = self.current()
            
            # Verificar si la key está deshabilitada por límite de requests
            usage = self.usage.get(key)
            if usage and usage.is_disabled():
                if not self.rotate():
                    raise RuntimeError("Todas las keys están deshabilitadas por límite de requests.")
                rotations += 1
                if max_rotations is not None and rotations >= max_rotations:
                    raise RuntimeError("No se encontró una Brave API key funcional dentro del límite de rotaciones.")
                continue
            
            st = self.check_key(key)
            if st.ok:
                return key

            # Si es error temporal, espera un poco y prueba sin rotar (por si Brave está inestable)
            if st.reason in ("SERVER_ERROR", "NETWORK_ERROR"):
                time.sleep(1.0 + random.uniform(0.2, 0.8))
                continue

            # Si es rate limit o key inválida, rota
            if st.reason in ("RATE_LIMIT", "UNAUTHORIZED_OR_FORBIDDEN"):
                if not self.rotate():
                    raise RuntimeError("Se agotaron las Brave API keys (todas rate-limited o inválidas).")
                rotations += 1
                if max_rotations is not None and rotations >= max_rotations:
                    raise RuntimeError("No se encontró una Brave API key funcional dentro del límite de rotaciones.")
                continue

            # Cualquier otro: rota una vez; si no hay, falla
            if not self.rotate():
                raise RuntimeError(f"Brave API keys fallando. Último status: {st.status_code} ({st.reason})")
            rotations += 1

    @staticmethod
    def from_env() -> "BraveKeyManager":
        # 1) BRAVE_API_KEYS=key1,key2,key3
        keys_csv = os.getenv("BRAVE_API_KEYS", "").strip()
        keys: list[str] = []

        if keys_csv:
            keys = [k.strip() for k in keys_csv.split(",") if k.strip()]

        # 2) BRAVE_API_KEY_1, BRAVE_API_KEY_2, ...
        if not keys:
            i = 1
            while True:
                k = os.getenv(f"BRAVE_API_KEY_{i}", "").strip()
                if not k:
                    break
                keys.append(k)
                i += 1

        # 3) fallback: BRAVE_API_KEY
        if not keys:
            k = os.getenv("BRAVE_API_KEY", "").strip()
            if k:
                keys = [k]

        return BraveKeyManager(keys)


def brave_search_with_rotation(
    key_manager: BraveKeyManager,
    query: str,
    count: int = 8,
    max_retries: int = 5
) -> list[dict]:
    """
    Búsqueda Brave con:
    - reintentos/backoff
    - rotación de keys en 429 / 401 / 403
    - conteo de requests por key
    - deshabilitación temporal al alcanzar 980 requests
    """
    last_err = None

    for attempt in range(max_retries):
        key = key_manager.ensure_working_key()
        headers = {"Accept": "application/json", "X-Subscription-Token": key}
        params = {"q": query, "count": count}

        try:
            r = requests.get(BRAVE_ENDPOINT, headers=headers, params=params, timeout=30)

            # 429: rota key y reintenta
            if r.status_code == 429:
                if not key_manager.rotate():
                    raise RuntimeError("Rate limit en todas las keys (429).")
                time.sleep((2 ** attempt) + random.uniform(0.2, 1.2))
                continue

            # 401/403: key inválida/no autorizada -> rota y reintenta
            if r.status_code in (401, 403):
                if not key_manager.rotate():
                    raise RuntimeError("Todas las keys están inválidas/no autorizadas (401/403).")
                time.sleep(0.6 + random.uniform(0.2, 0.8))
                continue

            # 5xx: backoff sin rotar
            if 500 <= r.status_code < 600:
                time.sleep((2 ** attempt) + random.uniform(0.2, 1.2))
                continue

            r.raise_for_status()
            data = r.json()

            # Incrementar contador de requests SOLO después de una respuesta exitosa
            key_manager._increment_current_key_usage()

            results = []
            for item in (data.get("web", {}) or {}).get("results", []) or []:
                results.append({
                    "url": item.get("url", "") or "",
                    "title": item.get("title", "") or "",
                    "snippet": item.get("description", "") or "",
                })
            return results

        except Exception as e:
            last_err = e
            time.sleep((2 ** attempt) + random.uniform(0.2, 1.2))

    raise RuntimeError(f"Brave search falló tras {max_retries} reintentos. Último error: {last_err}")