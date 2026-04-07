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
from utils.nombres import variantes_nombres, variantes_nombres_cr
from utils.pais_config import build_query
from utils.lotes import procesar_dividir


# -------------------------- CONFIG --------------------------

DELAY_API_MIN = 1.2
DELAY_API_MAX = 2.5
DELAY_ROW = 0.8
SAVE_EVERY = 10

RETRY_SR = True ##True para poder reintentar aquellos amrcados como no encontrados

# Si encontramos score >= este valor → cortamos búsqueda
EARLY_STOP_SCORE = 92


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


# -------------------------- Variantes de nombre --------------------------

def mejores_variantes_para_query(nombre_completo: str) -> list[str]:
    parts = [p for p in (nombre_completo or "").split() if p]
    if len(parts) < 3:
        return parts[:1] if parts else []

    ap1 = parts[0]
    ap2 = parts[1]
    nombres = parts[2:]

    n1 = nombres[0] if len(nombres) >= 1 else ""
    n2 = nombres[1] if len(nombres) >= 2 else ""

    variants = []

    if n1:
        variants.append(f"{n1} {ap1}")
        variants.append(f"{n1} {ap1} {ap2}")

    if n2:
        variants.append(f"{n2} {ap1}")

    out, seen = [], set()
    for v in variants:
        v = v.strip()
        if v and v.lower() not in seen:
            out.append(v)
            seen.add(v.lower())

    return out[:3]


# -------------------------- Excel helpers --------------------------

# Columnas de salida fijas
_OUTPUT_COLS: dict[str, object] = {
    "LinkedIn": "",
    "Score": 0,
    "Confianza": "",
    "Estudia_o_Estudio": "",
    "Candidatos_Top3": "",
    "Match_UDLA": "",
    "Match_Carrera": "",
    "Match_Anio": "",
}

_OUTPUT_STR_COLS = [
    "LinkedIn", "Confianza", "Estudia_o_Estudio",
    "Candidatos_Top3", "Match_UDLA", "Match_Carrera", "Match_Anio",
]


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col, default in _OUTPUT_COLS.items():
        if col not in df.columns:
            df[col] = default
    for col in _OUTPUT_STR_COLS:
        df[col] = df[col].astype("string")
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce").fillna(0).astype(int)
    # Cedula y Carnet pueden venir como float64 al releer el output — forzar a string
    for col in ("Cedula", "Carnet"):
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def is_already_filled(v: str) -> bool:
    v = safe_str(v)
    return v.startswith("http") and "linkedin.com" in v.lower()


def resolve_col(row, *candidates: str) -> str:
    """Retorna el valor del primer candidato de columna que exista y no esté vacío."""
    for col in candidates:
        val = safe_str(row.get(col))
        if val:
            return val
    return ""


# -------------------------- Procesamiento fila --------------------------

def process_row(estudiante: str, carrera: str, key_manager, cache: dict, anio_graduacion=None, pais: str = "EC", universidad: str = ""):

    try:
        _gen = variantes_nombres_cr if pais == "CR" else variantes_nombres
        variants = _gen(estudiante, max_variantes=3)
        if not variants:
            return empty_result()

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
                max_retries=5,
            )
            cache[ck] = results
            return results

        def evaluate(results):
            nonlocal scored_all

            for it in results:
                if not is_profile_url(it.get("url", "")):
                    continue

                s, flags, _ = score_candidate(carrera, estudiante, it, anio_graduacion, pais=pais, universidad=universidad)
                s = int(s)

                if s <= 0:
                    continue

                nc = name_closeness_for_item(estudiante, it, pais=pais)
                scored_all.append((s, nc, it, flags))

                # Early stop si encontramos score muy alto
                if s >= EARLY_STOP_SCORE:
                    return True

            return False

        for v in variants:

            # Fase 1 (rápida)
            q1 = build_query(v, carrera, mode="name_udlaec", pais=pais, universidad=universidad)
            stop = evaluate(fetch_query(q1, 15))
            if stop:
                break

            # Fase 2 (estricta)
            q2 = build_query(v, carrera, mode="strict", pais=pais, universidad=universidad, max_terms=4)
            stop = evaluate(fetch_query(q2, 10))
            if stop:
                break

        if not scored_all:
            return empty_result()

        # TOP3 por score
        top3_by_score = sorted(scored_all, key=lambda x: x[0], reverse=True)[:3]

        # BEST por closeness + score
        best = sorted(scored_all, key=lambda x: (x[1], x[0]), reverse=True)[0]
        best_score, _, best_item, best_flags = best

        # Umbrales de confianza por país
        # CR es más estricto porque el gating de universidad es menos específico
        if pais == "CR":
            thr_alta, thr_revisar = 90, 75
        else:
            thr_alta, thr_revisar = 85, 65

        if best_score >= thr_alta and best_flags.get("carrera") and best_flags.get("udla"):
            conf = "ALTA"
        elif best_score >= thr_revisar:
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
        "match_anio": "",
    }


def build_output(best_score, best_item, best_flags, top3_by_score, conf):

    top3 = " ; ".join([f"{x[2].get('url','')}|{x[0]}" for x in top3_by_score])

    study = infer_study_status(
        (best_item.get("title", "") or "") + " " +
        (best_item.get("snippet", "") or "")
    )

    # ALTA y REVISAR muestran la URL para revisión manual; BAJA queda vacía
    best_url = (best_item.get("url", "") or "") if conf in ("ALTA", "REVISAR") else ""

    anio_status = best_flags.get("anio")  # "MATCH", "NO COINCIDE", o None
    match_anio = anio_status if anio_status else "SIN INFO"

    return {
        "best_url": best_url,
        "score": int(best_score),
        "conf": conf,
        "study": study,
        "top3": top3,
        "match_udla": "SI" if best_flags.get("udla") else "NO/DUDOSO",
        "match_carrera": "SI" if best_flags.get("carrera") else "NO/DUDOSO",
        "match_anio": match_anio,
    }


# -------------------------- Lotes --------------------------

def run_lote(input_xlsx, output_xlsx, key_manager, cache, cache_path, pais: str = "EC"):

    if Path(output_xlsx).exists():
        print("Reanudando desde archivo existente...")
        df = pd.read_excel(output_xlsx)
    else:
        df = pd.read_excel(input_xlsx)
        df = ensure_columns(df)
        df.to_excel(output_xlsx, index=False)

    df = ensure_columns(df)

    # Recolectar URLs ya asignadas para evitar asignar la misma URL a dos personas
    def _norm_url(u: str) -> str:
        return (u or "").split("?")[0].rstrip("/").lower()

    assigned_urls: set[str] = set()
    for _, row in df.iterrows():
        v = safe_str(row.get("LinkedIn"))
        if is_already_filled(v):
            assigned_urls.add(_norm_url(v))

    for i, row in df.iterrows():

        linkedin_val = safe_str(row.get("LinkedIn"))
        score_val = int(row.get("Score") or 0)
        conf_val = safe_str(row.get("Confianza"))

        if is_already_filled(linkedin_val) and score_val > 0 and conf_val:
            continue

        if (not RETRY_SR) and (linkedin_val == "SR"):
            continue

        # Nombre del estudiante: acepta columnas en minúsculas (CR) o title-case (EC)
        estudiante = resolve_col(row, "nombre", "Estudiante", "NOMBRE")
        carrera    = resolve_col(row, "carrera", "Carrera", "CARRERA")

        if not estudiante or not carrera:
            continue

        anio_graduacion = resolve_col(row, "anio_graduacion", "Anio_Graduacion", "año_graduacion")

        # Universidad: CR usa el campo del Excel; EC no lo necesita
        universidad = resolve_col(row, "universidad", "Universidad") if pais == "CR" else ""

        out = process_row(estudiante, carrera, key_manager, cache, anio_graduacion, pais=pais, universidad=universidad)

        # Si la URL ya fue asignada a otra persona en este lote, no reutilizar
        url_norm = _norm_url(out["best_url"])
        if out["best_url"] and url_norm in assigned_urls:
            out["best_url"] = ""
            out["conf"] = "REVISAR"

        if out["best_url"]:
            assigned_urls.add(url_norm)

        df.at[i, "LinkedIn"] = out["best_url"] if out["best_url"] else "SR"
        df.at[i, "Score"] = out["score"]
        df.at[i, "Confianza"] = out["conf"]
        df.at[i, "Estudia_o_Estudio"] = out["study"]
        df.at[i, "Candidatos_Top3"] = out["top3"]
        df.at[i, "Match_UDLA"] = out["match_udla"]
        df.at[i, "Match_Carrera"] = out["match_carrera"]
        df.at[i, "Match_Anio"] = out["match_anio"]

        # Cedula y Carnet: copiar desde el input si existen en cualquier variante de nombre
        for dest, *srcs in [("Cedula", "cedula", "Cedula"), ("Carnet", "carnet", "Carnet")]:
            val = resolve_col(row, *srcs)
            if val:
                if dest not in df.columns:
                    df[dest] = pd.array([""] * len(df), dtype="string")
                df.at[i, dest] = val

        if (i + 1) % SAVE_EVERY == 0:
            print(f"💾 Guardando progreso en fila {i+1}")
            df.to_excel(output_xlsx, index=False)
            save_cache(cache, cache_path)

        time.sleep(DELAY_ROW)

    df.to_excel(output_xlsx, index=False)
    save_cache(cache, cache_path)

    print("✅ Lote completado correctamente.")


# -------------------------- MAIN --------------------------

PAISES = {
    "1": {"codigo": "EC", "nombre": "Ecuador",    "carpeta": "Ecuador"},
    "2": {"codigo": "CR", "nombre": "Costa Rica", "carpeta": "Costa_Rica"},
}


def select_pais() -> dict:
    print("\n=== BAVE Search — Seleccion de pais ===")
    for key, info in PAISES.items():
        print(f"  {key}. {info['nombre']} ({info['codigo']})")
    print()

    while True:
        opcion = input("Selecciona el pais [1/2]: ").strip()
        if opcion in PAISES:
            seleccion = PAISES[opcion]
            print(f"-> Procesando: {seleccion['nombre']} ({seleccion['codigo']})\n")
            return seleccion
        print("Opcion invalida. Ingresa 1 o 2.")


def main():
    load_dotenv()

    pais_info = select_pais()
    pais = pais_info["codigo"]
    carpeta = pais_info["carpeta"]

    key_manager = SerperKeyManager.from_env()

    base_dir = f"data/{carpeta}"
    in_dir   = Path(f"{base_dir}/lotes")
    out_dir  = Path(f"{base_dir}/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Dividir archivos nuevos en dividir/ → lotes/ antes de procesar
    generados = procesar_dividir(base_dir)
    if generados:
        print(f"[dividir] {generados} lote(s) generado(s) desde carpeta 'dividir'")

    cache_path = f"cache/serper_cache_{pais.lower()}.json"
    cache = load_cache(cache_path)

    lotes = sorted(in_dir.glob("*.xlsx"))
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
            pais=pais,
        )

    print("Listo. Todos los lotes procesados.")


if __name__ == "__main__":
    main()