import json
from pathlib import Path

def load_cache(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_cache(cache: dict, path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def cache_get(cache: dict, key: str):
    return cache.get(key)

def cache_set(cache: dict, key: str, value):
    cache[key] = value
