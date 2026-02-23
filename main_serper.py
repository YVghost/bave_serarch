# main_run_lotes_serper.py
# Versión usando SERPER como motor de búsqueda
# Mantiene arquitectura por FASES + early stop

from __future__ import annotations

import time
import json
from pathlib import Path
import random

import pandas as pd
from dotenv import load_dotenv

from utils.serper_client import SerperKeyManager, serper_search_with_rotation
from utils.nombres import variantes_nombres
from utils.scoring import is_profile_url, score_candidate, infer_study_status
from utils.carreras_synonyms import expand_career_keywords


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
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


# -------------------------- Queries --------------------------

def build_q1(nombre_var: str) -> str:
    return f'site:linkedin.com/in ("{nombre_var}") (UDLA OR "Universidad de Las Américas") Ecuador'

def build_q2(nombre_var: str, carrera: str, max_terms: int = 6) -> str:
    kws = expand_career_keywords(carrera) or []

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
    extras = {
        "Score": "",
        "Confianza": "",
        "Estudia_o_Estudio": "",
        "Candidatos_Top3": "",
        "Match_UDLA": "",
        "Match_Carrera": "",
    }
    for col, default in extras.items():
        if col not in df.columns:
            df[col] = default
    return df

def is_already_filled(v: str) -> bool:
    v = (v or "").strip()
    return v.startswith("http") and "linkedin.com" in v.lower()


# -------------------------- Procesamiento por fila --------------------------

def process_row(
    estudiante: str,
    carrera: str,
    key_manager: SerperKeyManager,
    cache: dict,
    per_query_count_q1: int = 5,
    per_query_count_q2: int = 8,
    max_candidates: int = 25,
    base_sleep_s: float = 0.25,
    thr_accept: int = 85,
    thr_try_more: int = 65,
    thr_run_q2: int = 75,
) -> dict:

    variants = variantes_nombres(estudiante, max_variantes=3)
    if not variants:
        return {
            "best_url": "", "score": 0, "conf": "NO_ENCONTRADO",
            "study": "NO DETERMINADO", "top3": "",
            "match_udla": "", "match_carrera": ""
        }

    best = None
    scored_all: list[tuple[int, dict, dict]] = []

    def pace():
        time.sleep(base_sleep_s + random.uniform(0.05, 0.25))

    def fetch_query(q: str, count: int) -> list[dict]:
        ck = f"serper::{count}::{q}"
        if ck in cache:
            return cache[ck]

        results = serper_search_with_rotation(
            key_manager=key_manager,
            query=q,
            count=count,
            max_retries=5
        )

        cache[ck] = results
        return results

    def eval_results(results_use: list[dict]) -> dict | None:
        nonlocal best, scored_all

        profiles = [it for it in results_use if is_profile_url(it.get("url", ""))]
        if not profiles:
            return None

        for it in profiles[:max_candidates]:
            s, flags, _ = score_candidate(carrera, estudiante, it)
            scored_all.append((s, it, flags))

            if best is None or s > best[0]:
                best = (s, it, flags)

            if s >= thr_accept and flags.get("udla") and flags.get("carrera"):
                study = infer_study_status(
                    (it.get("title", "") or "") + " " + (it.get("snippet", "") or "")
                )
                scored_all.sort(key=lambda x: x[0], reverse=True)
                top3 = " ; ".join([f"{x[1].get('url','')}|{x[0]}" for x in scored_all[:3]])

                return {
                    "best_url": it.get("url", ""),
                    "score": int(s),
                    "conf": "ALTA",
                    "study": study,
                    "top3": top3,
                    "match_udla": "SI",
                    "match_carrera": "SI",
                }

        return None


    v1 = variants[0]

    q1 = build_q1(v1)
    stopped = eval_results(fetch_query(q1, per_query_count_q1))
    if stopped:
        return stopped
    pace()

    if best and best[0] >= thr_run_q2:
        best_s, best_item, best_flags = best
        scored_all.sort(key=lambda x: x[0], reverse=True)
        top3 = " ; ".join([f"{x[1].get('url','')}|{x[0]}" for x in scored_all[:3]])
        study = infer_study_status(
            (best_item.get("title", "") or "") + " " + (best_item.get("snippet", "") or "")
        )

        return {
            "best_url": "",
            "score": int(best_s),
            "conf": "REVISAR",
            "study": study,
            "top3": top3,
            "match_udla": "SI" if best_flags.get("udla") else "NO/DUDOSO",
            "match_carrera": "SI" if best_flags.get("carrera") else "NO/DUDOSO",
        }

    q2 = build_q2(v1, carrera)
    stopped = eval_results(fetch_query(q2, per_query_count_q2))
    if stopped:
        return stopped
    pace()

    if not best:
        return {
            "best_url": "", "score": 0, "conf": "NO_ENCONTRADO",
            "study": "NO DETERMINADO", "top3": "",
            "match_udla": "", "match_carrera": ""
        }

    best_s, best_item, best_flags = best
    scored_all.sort(key=lambda x: x[0], reverse=True)
    top3 = " ; ".join([f"{x[1].get('url','')}|{x[0]}" for x in scored_all[:3]])
    study = infer_study_status(
        (best_item.get("title", "") or "") + " " + (best_item.get("snippet", "") or "")
    )

    if best_s >= thr_accept and best_flags.get("udla") and best_flags.get("carrera"):
        conf, best_url = "ALTA", best_item.get("url", "")
    elif best_s >= thr_try_more:
        conf, best_url = "REVISAR", ""
    else:
        conf, best_url = "BAJA", ""

    return {
        "best_url": best_url,
        "score": int(best_s),
        "conf": conf,
        "study": study,
        "top3": top3,
        "match_udla": "SI" if best_flags.get("udla") else "NO/DUDOSO",
        "match_carrera": "SI" if best_flags.get("carrera") else "NO/DUDOSO",
    }


# -------------------------- Lotes --------------------------

def run_lote(input_xlsx: str, output_xlsx: str,
             key_manager: SerperKeyManager,
             cache: dict, cache_path: str):

    df = pd.read_excel(input_xlsx)
    df = ensure_columns(df)

    for i, row in df.iterrows():
        linkedin_val = str(row.get("LinkedIn", "") or "").strip()
        if is_already_filled(linkedin_val) or linkedin_val == "SR":
            continue

        estudiante = str(row.get("Estudiante", "") or "")
        carrera = str(row.get("Carrera", "") or "")
        if not estudiante or not carrera:
            continue

        out = process_row(estudiante, carrera, key_manager, cache)

        df.at[i, "LinkedIn"] = out["best_url"] if out["best_url"] else "SR"
        df.at[i, "Score"] = out["score"]
        df.at[i, "Confianza"] = out["conf"]
        df.at[i, "Estudia_o_Estudio"] = out["study"]
        df.at[i, "Candidatos_Top3"] = out["top3"]
        df.at[i, "Match_UDLA"] = out["match_udla"]
        df.at[i, "Match_Carrera"] = out["match_carrera"]

        if (i + 1) % 50 == 0:
            save_cache(cache, cache_path)

    save_cache(cache, cache_path)
    df.to_excel(output_xlsx, index=False)


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

        if out_file.exists():
            print(f"[SKIP] Ya existe: {out_file.name}")
            continue

        print(f"[RUN] {f.name} -> {out_file.name}")

        run_lote(
            input_xlsx=str(f),
            output_xlsx=str(out_file),
            key_manager=key_manager,
            cache=cache,
            cache_path=cache_path,
        )

    print("Listo. Todos los lotes procesados.")


if __name__ == "__main__":
    main()