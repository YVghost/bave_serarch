# main_run_lotes.py
# Optimización aplicada para minimizar costo:
# - Arquitectura por FASES + early stop
# - 1 sola query al inicio (q1) con 1 variante
# - Solo si NO hay match fuerte, hace q2 (con carrera) y/o más variantes
# - Reduce requests promedio por estudiante de ~6 a ~1–2 (dependiendo de casos difíciles)
#
# Nota: mantiene cache + rotación de API keys + top3 para auditoría

from __future__ import annotations

import time
import json
from pathlib import Path
import random

import pandas as pd
from dotenv import load_dotenv

from utils.api_keys import BraveKeyManager, brave_search_with_rotation
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


# -------------------------- Queries (FASES) --------------------------

def build_q1(nombre_var: str) -> str:
    # Query fuerte base (barata)
    return f'site:linkedin.com/in ("{nombre_var}") (UDLA OR "Universidad de Las Américas") Ecuador'

def build_q2(nombre_var: str, carrera: str, max_terms: int = 6) -> str:
    """
    Query con carrera, usando keywords expandidas (ES/EN/etc.) como OR.
    max_terms limita el tamaño para no matar el recall ni encarecer.
    """
    kws = expand_career_keywords(carrera) or []
    # quita duplicados, vacíos y deja pocas
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

    # fallback: si por alguna razón no hay kws, usa carrera tal cual
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


# -------------------------- Procesamiento por fila (FASES + EARLY STOP) --------------------------

def process_row(
    estudiante: str,
    carrera: str,
    key_manager: BraveKeyManager,
    cache: dict,
    per_query_count: int = 8,
    max_candidates: int = 25,
    base_sleep_s: float = 0.25,
    thr_accept: int = 85,        # acepta automático si >=85 y flags ok
    thr_try_more: int = 65       # si <65, vale la pena probar otra fase/variante
) -> dict:
    """
    FASE 1:
      - Solo variante 1
      - Solo q1 (UDLA/Ecuador)
      - Si ALTA => retorna

    FASE 2:
      - Variante 1
      - q2 (con carrera)
      - Si ALTA => retorna
      - Si score sigue bajo => pasar a variantes 2/3

    FASE 3:
      - Variantes 2 y 3 (solo si realmente hace falta)
      - primero q1, luego q2
      - early stop en cualquier fase si ALTA
    """

    variants = variantes_nombres(estudiante, max_variantes=3)
    if not variants:
        return {
            "best_url": "",
            "score": 0,
            "conf": "NO_ENCONTRADO",
            "study": "NO DETERMINADO",
            "top3": "",
            "match_udla": "",
            "match_carrera": "",
        }

    best = None  # (score, item, flags)
    scored_all: list[tuple[int, dict, dict]] = []  # (score, item, flags)

    def eval_results(results_use: list[dict]) -> dict | None:
        nonlocal best, scored_all

        profiles = [it for it in results_use if is_profile_url(it.get("url", ""))]
        if not profiles:
            return None

        for it in profiles[:max_candidates]:
            s, flags, _reasons = score_candidate(carrera, estudiante, it)
            scored_all.append((s, it, flags))

            if best is None or s > best[0]:
                best = (s, it, flags)

            if s >= thr_accept and flags.get("udla") and flags.get("carrera"):
                study = infer_study_status((it.get("title", "") or "") + " " + (it.get("snippet", "") or ""))
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

    def fetch_query(q: str) -> list[dict]:
        ck = f"brave::{q}"
        if ck in cache:
            return cache[ck]

        results_use = brave_search_with_rotation(
            key_manager=key_manager,
            query=q,
            count=per_query_count,
            max_retries=5
        )
        cache[ck] = results_use
        return results_use

    def pace():
        time.sleep(base_sleep_s + random.uniform(0.05, 0.25))

    # ---------------- FASE 1: variante 1, q1 ----------------
    v1 = variants[0]
    q = build_q1(v1)
    stopped = eval_results(fetch_query(q))
    if stopped:
        return stopped
    pace()

    # Si ya tengo algo decente (>=65) pero no ALTA, paso directo a q2 con misma variante
    # Si está bajísimo, igual pruebo q2 (a veces la carrera levanta el score)
    q = build_q2(v1, carrera, max_terms=6)
    stopped = eval_results(fetch_query(q))
    if stopped:
        return stopped
    pace()

    # Si tras variante 1, el mejor score ya es >= thr_try_more (65),
    # normalmente es "REVISAR" y NO vale gastar más requests en otras variantes.
    # Así ahorras bastante.
    if best and best[0] >= thr_try_more:
        best_s, best_item, best_flags = best
        scored_all.sort(key=lambda x: x[0], reverse=True)
        top3 = " ; ".join([f"{x[1].get('url','')}|{x[0]}" for x in scored_all[:3]])
        study = infer_study_status((best_item.get("title", "") or "") + " " + (best_item.get("snippet", "") or ""))
        return {
            "best_url": "",
            "score": int(best_s),
            "conf": "REVISAR",
            "study": study,
            "top3": top3,
            "match_udla": "SI" if best_flags.get("udla") else "NO/DUDOSO",
            "match_carrera": "SI" if best_flags.get("carrera") else "NO/DUDOSO",
        }

    # ---------------- FASE 3: solo si realmente está bajo (<65), prueba variantes 2 y 3 ----------------
    for nv in variants[1:]:
        # q1
        q = build_q1(nv)
        stopped = eval_results(fetch_query(q))
        if stopped:
            return stopped
        pace()

        # q2
        q = build_q2(nv, carrera)
        stopped = eval_results(fetch_query(q))
        if stopped:
            return stopped
        pace()

        # Si ya subió a >=65, corta para no gastar más
        if best and best[0] >= thr_try_more:
            break

    # ---------------- Resultado final ----------------
    if not best:
        return {
            "best_url": "",
            "score": 0,
            "conf": "NO_ENCONTRADO",
            "study": "NO DETERMINADO",
            "top3": "",
            "match_udla": "",
            "match_carrera": "",
        }

    best_s, best_item, best_flags = best
    scored_all.sort(key=lambda x: x[0], reverse=True)
    top3 = " ; ".join([f"{x[1].get('url','')}|{x[0]}" for x in scored_all[:3]])
    study = infer_study_status((best_item.get("title", "") or "") + " " + (best_item.get("snippet", "") or ""))

    if best_s >= thr_accept and best_flags.get("udla") and best_flags.get("carrera"):
        conf = "ALTA"
        best_url = best_item.get("url", "")
    elif best_s >= thr_try_more:
        conf = "REVISAR"
        best_url = ""
    else:
        conf = "BAJA"
        best_url = ""

    return {
        "best_url": best_url,
        "score": int(best_s),
        "conf": conf,
        "study": study,
        "top3": top3,
        "match_udla": "SI" if best_flags.get("udla") else "NO/DUDOSO",
        "match_carrera": "SI" if best_flags.get("carrera") else "NO/DUDOSO",
    }


# -------------------------- Procesamiento por lote --------------------------

def run_lote(
    input_xlsx: str,
    output_xlsx: str,
    key_manager: BraveKeyManager,
    cache: dict,
    cache_path: str,
    per_query_count: int = 8,
    base_sleep_s: float = 0.25,
) -> None:
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

        out = process_row(
            estudiante=estudiante,
            carrera=carrera,
            key_manager=key_manager,
            cache=cache,
            per_query_count=per_query_count,
            base_sleep_s=base_sleep_s
        )

        # escribe solo si ALTA
        if out["conf"] == "ALTA" and out["best_url"]:
            df.at[i, "LinkedIn"] = out["best_url"]
        else:
            df.at[i, "LinkedIn"] = linkedin_val if linkedin_val else "SR"

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


# -------------------------- Main: iterar todos los lotes --------------------------

def main():
    load_dotenv()

    key_manager = BraveKeyManager.from_env()
    key_manager.ensure_working_key()

    in_dir = Path("data/lotes")
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    cache_path = "cache/brave_cache.json"
    cache = load_cache(cache_path)

    per_query_count = 8
    base_sleep_s = 0.25

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
            per_query_count=per_query_count,
            base_sleep_s=base_sleep_s,
        )

    print("Listo. Todos los lotes procesados.")


if __name__ == "__main__":
    main()
