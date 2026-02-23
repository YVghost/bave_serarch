import os
import requests
import time

class SerperKeyManager:
    def __init__(self, keys: list[str]):
        self.keys = keys
        self.index = 0

    @classmethod
    def from_env(cls):
        raw = os.getenv("SERPER_API_KEYS", "")
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if not keys:
            raise RuntimeError("No SERPER_API_KEYS found in .env")
        return cls(keys)

    def get_key(self):
        return self.keys[self.index]

    def rotate(self):
        self.index = (self.index + 1) % len(self.keys)

def serper_search_with_rotation(
    key_manager: SerperKeyManager,
    query: str,
    count: int = 10,
    max_retries: int = 3
):
    url = "https://google.serper.dev/search"

    for _ in range(max_retries):
        api_key = key_manager.get_key()

        headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json"
        }

        payload = {
            "q": query,
            "num": count
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)

            if resp.status_code == 200:
                data = resp.json()
                results = []

                for item in data.get("organic", []):
                    results.append({
                        "url": item.get("link", ""),
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", "")
                    })

                return results

            if resp.status_code in (401, 429):
                key_manager.rotate()
                time.sleep(1)
                continue

        except Exception:
            time.sleep(1)

    return []