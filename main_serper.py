from __future__ import annotations

import time
import json
from pathlib import Path
import random
import traceback

import pandas as pd
from dotenv import load_dotenv

from utils.serper_client import SerperKeyManager, serper_search_with_rotation
from utils.scoring import (
    is_profile_url,
    score_candidate,
    infer_study_status,
    name_closeness_for_item,
)
from utils.carreras_synonyms import expand_carrera_keywords


# -------------------------- CONFIG --------------------------

DELAY_API_MIN = 1.2
DELAY_API_MAX = 2.5
DELAY_ROW = 0.8
SAVE_EVERY = 10

# Si False: NO reintenta filas SR (modo producción)
# Si True : reintenta SR (modo calibración)
RETRY_SR = True

# País esperado para gating (scoring)
EXPECTED_COUNTRY = "ec"


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


# -------------------------- Variantes de nombre (DATASET: AP1 AP2 N1 N2...) --------------------------

def mejores_variantes_para_query(nombre_completo: str) -> list[str]:
    """
    Dataset: APELLIDO1 APELLIDO2 NOMBRE1 NOMBRE2...

    Queremos variantes "linkedin-friendly":
    - N1 AP1
    - N1 AP1 AP2
    - N2 AP1 (si existe)

    Esto reduce consumo y mejora recall.
    """
    parts = [p for p in (nombre_completo or "").split() if p]
    if len(parts) < 3:
        return parts[:1] if parts else []

    ap1 = parts[0]
    ap2 = parts[1] if len(parts) >= 2 else ""
    nombres = parts[2:] if len(parts) >= 3 else []

    n1 = nombres[0] if len(nombres) >= 1 else ""
    n2 = nombres[1] if len(nombres) >= 2 else ""

    variants = []

    if n1 and ap1:
        variants.append(f"{n1} {ap1}".strip())
        variants.append(f"{n1} {ap1} {ap2}".strip())

    if n2 and ap1:
        variants.append(f"{n2} {ap1}".strip())

    # dedupe preservando orden
    out, seen = [], set()
    for v in variants:
        k = v.lower()
        if k and k not in seen:
            out.append(v)
            seen.add(k)

    return out[:3]


# -------------------------- Query builder (2 fases) --------------------------

def build_query(nombre_var: str, carrera: str, mode: str, max_terms: int = 4) -> str:
    """
    mode:
      - "name_ec": solo nombre + Ecuador (rápido para descubrir el perfil correcto)
      - "strict": nombre + carrera + UDLA + Ecuador (validación fuerte)
    """
    base = f'site:linkedin.com/in ("{nombre_var}")'

    # Query ligera: encontrar perfiles por nombre + geo
    if mode == "name_ec":
        return base + ' (Ecuador OR Quito OR Guayaquil OR Cuenca OR Ambato OR Manta)'

    # Query estricta: carrera + UDLA + geo
    kws = expand_carrera_keywords(carrera) or []
    picked, seen = [], set()
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

    carrera_part = "(" + " OR ".join([f'"{k}"' for k in picked]) + ")" if picked else f'"{carrera}"'

    return (
        f'{base} '
        f'{carrera_part} '
        f'(UDLA OR "Universidad de Las Américas") '
        f'(Ecuador OR Quito OR Guayaquil OR Cuenca OR Ambato OR Manta)'
    )


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

    for col in ["LinkedIn", "Confianza", "Estudia_o_Estudio", "Candidatos_Top3", "Match_UDLA", "Match_Carrera"]:
        df[col] = df[col].astype("string")

    df["Score"] = pd.to_numeric(df["Score"], errors="coerce").fillna(0).astype(int)
    return df


def is_already_filled(v: str) -> bool:
    v = safe_str(v)
    return v.startswith("http") and "linkedin.com" in v.lower()


# -------------------------- Procesamiento fila --------------------------

def process_row(estudiante: str, carrera: str, key_manager, cache: dict, expected_country: str = EXPECTED_COUNTRY):
    """
    Adaptado al NUEVO scoring:

    - score_candidate ya contiene gates estrictos:
      * perfil real
      * apellido en slug
      * al menos 1 nombre
      * Ecuador (según expected_country)
      * UDLA obligatorio

    - Guardamos TOP3 por SCORE.
    - Elegimos BEST por (name_closeness, score) para evitar falsos positivos.
    """
    try:
        variants = mejores_variantes_para_query(estudiante)
        if not variants:
            return empty_result()

        # scored_all items:
        # (score, name_close, item, flags)
        scored_all: list[tuple[int, int, dict, dict]] = []

        def fetch_query(q: str, count: int):
            ck = f"{count}::{q}"
            if ck in cache:
                return cache[ck]

            wait_api()
            results = serper_search_with_rotation(
                key_manager=key_manager,
                query=q,
                count=count,
                max_retries=5,
            )
            cache[ck] = results
            return results

        def evaluate(results):
            profiles = [it for it in results if is_profile_url(it.get("url", ""))]
            if not profiles:
                return

            for it in profiles[:50]:
                s, flags, _ = score_candidate(
                    carrera,
                    estudiante,
                    it,
                    expected_country=expected_country
                )
                s = int(s)

                # gates estrictos => s>0 significa que pasó validación
                if s <= 0:
                    continue

                nc = name_closeness_for_item(estudiante, it)
                scored_all.append((s, nc, it, flags))

        # 2 fases por variante:
        # - "name_ec" trae candidatos por nombre+geo (rápido)
        # - "strict" valida con carrera + UDLA + geo
        for v in variants:
            q1 = build_query(v, carrera, mode="name_ec")
            evaluate(fetch_query(q1, count=20))

            q2 = build_query(v, carrera, mode="strict", max_terms=4)
            evaluate(fetch_query(q2, count=10))

        if not scored_all:
            return empty_result()

        # TOP3 por SCORE (auditoría)
        top3_by_score = sorted(scored_all, key=lambda x: x[0], reverse=True)[:3]

        # BEST REAL por CLOSENESS y SCORE
        best = sorted(scored_all, key=lambda x: (x[1], x[0]), reverse=True)[0]
        best_score, best_nc, best_item, best_flags = best

        # Confianza (puedes endurecer umbrales si deseas)
        if best_score >= 85:
            conf = "ALTA"
        elif best_score >= 65:
            conf = "REVISAR"
        else:
            conf = "BAJA"

        return build_output(best_score, best_item, best_flags, top3_by_score, conf)

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


def build_output(best_score: int, best_item: dict, best_flags: dict, top3_by_score, conf: str):
    # top3_by_score: (score, name_close, item, flags)
    top3 = " ; ".join([f"{x[2].get('url','')}|{x[0]}" for x in top3_by_score])

    study = infer_study_status(
        (best_item.get("title", "") or "") + " " + (best_item.get("snippet", "") or "")
    )

    return {
        "best_url": best_item.get("url", "") or "",
        "score": int(best_score),
        "conf": conf,
        "study": study,
        "top3": top3,
        "match_udla": "SI" if best_flags.get("udla") else "NO/DUDOSO",
        "match_carrera": "SI" if best_flags.get("carrera") else "NO/DUDOSO",
    }


# -------------------------- Lotes con guardado incremental --------------------------

def run_lote(input_xlsx, output_xlsx, key_manager, cache, cache_path):
    if Path(output_xlsx).exists():
        print("Reanudando desde archivo existente...")
        df = pd.read_excel(output_xlsx)
    else:
        df = pd.read_excel(input_xlsx)
        df = ensure_columns(df)
        df.to_excel(output_xlsx, index=False)

    df = ensure_columns(df)

    for i, row in df.iterrows():
        try:
            linkedin_val = safe_str(row.get("LinkedIn"))
            score_val = int(row.get("Score") or 0)
            conf_val = safe_str(row.get("Confianza"))

            # Saltar si ya está finalizado (URL + score + confianza)
            if is_already_filled(linkedin_val) and score_val > 0 and conf_val:
                continue

            # No reintentar SR si RETRY_SR=False
            if (not RETRY_SR) and (linkedin_val == "SR"):
                continue

            estudiante = safe_str(row.get("Estudiante"))
            carrera = safe_str(row.get("Carrera"))

            if not estudiante or not carrera:
                continue

            out = process_row(estudiante, carrera, key_manager, cache, expected_country=EXPECTED_COUNTRY)

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