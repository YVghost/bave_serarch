from __future__ import annotations

import time
import json
from pathlib import Path
import random
import traceback

import pandas as pd
from dotenv import load_dotenv

from utils.serper_client import SerperKeyManager, serper_search_with_rotation
from utils.nombres import variantes_nombres
from utils.scoring import is_profile_url, score_candidate, infer_study_status
from utils.carreras_synonyms import expand_carrera_keywords


# -------------------------- CONFIG RATE LIMIT --------------------------

DELAY_API_MIN = 1.2
DELAY_API_MAX = 2.5
DELAY_ROW = 0.8
SAVE_EVERY = 10  # 🔥 guardar cada N filas


def wait_api():
    time.sleep(random.uniform(DELAY_API_MIN, DELAY_API_MAX))


# -------------------------- Cache --------------------------

def load_cache(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict, path: str) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# -------------------------- Safe helpers --------------------------

def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


# -------------------------- Queries --------------------------

def build_q1(nombre_var: str) -> str:
    return f'site:linkedin.com/in ("{nombre_var}") (UDLA OR "Universidad de Las Américas") Ecuador'


def build_q2(nombre_var: str, carrera: str, max_terms: int = 6) -> str:
    kws = expand_carrera_keywords(carrera) or []

    seen = set()
    picked = []

    for k in kws:
        k = (k or "").strip()
        if not k:
            continue
        nk = k.lower()
        if nk in seen:
            continue
        seen.add(nk)
        picked.append(k)
        if len(picked) >= max_terms:
            break

    if not picked:
        carrera_part = f'"{carrera}"'
    else:
        carrera_part = "(" + " OR ".join([f'"{k}"' for k in picked]) + ")"

    return f'site:linkedin.com/in ("{nombre_var}") {carrera_part} (UDLA OR "Universidad de Las Américas")'


# -------------------------- Excel helpers --------------------------

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "LinkedIn": "",
        "Estudiante": "",
        "Carrera": "",
        "Score": 0,
        "Confianza": "",
        "Estudia_o_Estudio": "",
        "Candidatos_Top3": "",
        "Match_UDLA": "",
        "Match_Carrera": "",
    }

    for col, default in required.items():
        if col not in df.columns:
            df[col] = default

    df["LinkedIn"] = df["LinkedIn"].astype("string")
    df["Confianza"] = df["Confianza"].astype("string")
    df["Estudia_o_Estudio"] = df["Estudia_o_Estudio"].astype("string")
    df["Candidatos_Top3"] = df["Candidatos_Top3"].astype("string")
    df["Match_UDLA"] = df["Match_UDLA"].astype("string")
    df["Match_Carrera"] = df["Match_Carrera"].astype("string")

    df["Score"] = pd.to_numeric(df["Score"], errors="coerce").fillna(0).astype(int)

    return df


def is_already_filled(v: str) -> bool:
    v = safe_str(v)
    return v.startswith("http") and "linkedin.com" in v.lower()


# -------------------------- Procesamiento fila --------------------------

def process_row(estudiante, carrera, key_manager, cache):

    try:
        variants = variantes_nombres(estudiante, max_variantes=3)
        if not variants:
            return empty_result()

        best = None
        scored_all = []

        def fetch_query(q: str, count: int):
            ck = f"{count}::{q}"
            if ck in cache:
                return cache[ck]

            wait_api()

            results = serper_search_with_rotation(
                key_manager=key_manager,
                query=q,
                count=count,
                max_retries=5
            )

            cache[ck] = results
            return results

        def evaluate(results):
            nonlocal best, scored_all

            profiles = [it for it in results if is_profile_url(it.get("url", ""))]
            if not profiles:
                return None

            for it in profiles[:25]:
                s, flags, _ = score_candidate(carrera, estudiante, it)
                s = int(s)

                scored_all.append((s, it, flags))

                if best is None or s > best[0]:
                    best = (s, it, flags)

                if s >= 85 and flags.get("udla") and flags.get("carrera"):
                    return build_output(s, it, flags, scored_all, "ALTA")

            return None

        v1 = variants[0]

        r = evaluate(fetch_query(build_q1(v1), 5))
        if r:
            return r

        if best and best[0] >= 75:
            return build_output(best[0], best[1], best[2], scored_all, "REVISAR")

        r = evaluate(fetch_query(build_q2(v1, carrera), 8))
        if r:
            return r

        if not best:
            return empty_result()

        if best[0] >= 85:
            conf = "ALTA"
        elif best[0] >= 65:
            conf = "REVISAR"
        else:
            conf = "BAJA"

        return build_output(best[0], best[1], best[2], scored_all, conf)

    except Exception:
        traceback.print_exc()
        return empty_result()


def empty_result():
    return {
        "best_url": "",
        "score": 0,
        "conf": "NO_ENCONTRADO",
        "study": "NO DETERMINADO",
        "top3": "",
        "match_udla": "",
        "match_carrera": "",
    }


def build_output(score, item, flags, scored_all, conf):
    scored_all.sort(key=lambda x: x[0], reverse=True)
    top3 = " ; ".join([f"{x[1].get('url','')}|{x[0]}" for x in scored_all[:3]])

    study = infer_study_status(
        (item.get("title", "") or "") + " " + (item.get("snippet", "") or "")
    )

    return {
        "best_url": item.get("url", "") if conf == "ALTA" else "",
        "score": int(score),
        "conf": conf,
        "study": study,
        "top3": top3,
        "match_udla": "SI" if flags.get("udla") else "NO/DUDOSO",
        "match_carrera": "SI" if flags.get("carrera") else "NO/DUDOSO",
    }


# -------------------------- Lotes con guardado incremental --------------------------

def run_lote(input_xlsx, output_xlsx, key_manager, cache, cache_path):

    if Path(output_xlsx).exists():
        print("Reanudando desde archivo existente...")
        df = pd.read_excel(output_xlsx)
    else:
        df = pd.read_excel(input_xlsx)
        df = ensure_columns(df)
        df.to_excel(output_xlsx, index=False)  # crear archivo inmediatamente

    df = ensure_columns(df)

    for i, row in df.iterrows():
        try:
            linkedin_val = safe_str(row.get("LinkedIn"))

            if is_already_filled(linkedin_val) or linkedin_val == "SR":
                continue

            estudiante = safe_str(row.get("Estudiante"))
            carrera = safe_str(row.get("Carrera"))

            if not estudiante or not carrera:
                continue

            out = process_row(estudiante, carrera, key_manager, cache)

            df.at[i, "LinkedIn"] = out["best_url"] if out["best_url"] else "SR"
            df.at[i, "Score"] = int(out["score"])
            df.at[i, "Confianza"] = str(out["conf"])
            df.at[i, "Estudia_o_Estudio"] = str(out["study"])
            df.at[i, "Candidatos_Top3"] = str(out["top3"])
            df.at[i, "Match_UDLA"] = str(out["match_udla"])
            df.at[i, "Match_Carrera"] = str(out["match_carrera"])

            if (i + 1) % SAVE_EVERY == 0:
                print(f"💾 Guardando progreso en fila {i+1}")
                df.to_excel(output_xlsx, index=False)
                save_cache(cache, cache_path)

            time.sleep(DELAY_ROW)

        except Exception:
            traceback.print_exc()
            print("⚠ Error detectado. Guardando progreso...")
            df.to_excel(output_xlsx, index=False)
            save_cache(cache, cache_path)
            continue

    df.to_excel(output_xlsx, index=False)
    save_cache(cache, cache_path)

    print("✅ Lote completado correctamente.")


# -------------------------- MAIN --------------------------

def main():
    load_dotenv()

    key_manager = SerperKeyManager.from_env()

    in_dir = Path("data/lotes")
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    cache_path = "cache/serper_cache.json"
    cache = load_cache(cache_path)

    lotes = sorted(in_dir.glob("estudiantes_lote_*.xlsx"))
    if not lotes:
        raise RuntimeError(f"No se encontraron lotes en: {in_dir.resolve()}")

    for f in lotes:
        out_file = out_dir / f"{f.stem}_enriquecido.xlsx"

        print(f"[RUN] {f.name} -> {out_file.name}")

        run_lote(
            input_xlsx=str(f),
            output_xlsx=str(out_file),
            key_manager=key_manager,
            cache=cache,
            cache_path=cache_path,
        )

    print("🎉 Listo. Todos los lotes procesados.")


if __name__ == "__main__":
    main()