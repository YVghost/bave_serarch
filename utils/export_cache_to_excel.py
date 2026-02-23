import json
import pandas as pd
from pathlib import Path


CACHE_PATH = "cache/serper_cache.json"
OUTPUT_PATH = "cache/serper_cache_export.xlsx"


def main():

    cache_file = Path(CACHE_PATH)

    if not cache_file.exists():
        print("No existe la cache.")
        return

    with open(cache_file, "r", encoding="utf-8") as f:
        cache = json.load(f)

    rows = []

    for key, results in cache.items():

        try:
            count, query = key.split("::", 1)
        except ValueError:
            continue

        if not isinstance(results, list):
            continue

        for pos, item in enumerate(results, start=1):

            rows.append({
                "Count": count,
                "Query": query,
                "Position": pos,
                "URL": item.get("url", ""),
                "Title": item.get("title", ""),
                "Snippet": item.get("snippet", "")
            })

    df = pd.DataFrame(rows)

    df.to_excel(OUTPUT_PATH, index=False)

    print(f"Exportado a: {OUTPUT_PATH}")
    print(f"Total registros: {len(df)}")


if __name__ == "__main__":
    main()