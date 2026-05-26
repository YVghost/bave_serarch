from __future__ import annotations

import time
import json
from pathlib import Path
import random
import traceback
import re

import pandas as pd
from dotenv import load_dotenv

from utils.serper_client import SerperKeyManager, serper_search_with_rotation
from utils.scoring import (
    is_profile_url,
    infer_study_status,
)
import utils.scoring as _scoring_ec
import utils.scoring_cr as _scoring_cr
from utils.nombres import variantes_nombres, variantes_nombres_cr
from utils.pais_config import build_query
from utils.lotes import procesar_dividir


DELAY_API_MIN = 1.2
DELAY_API_MAX = 2.5
DELAY_ROW = 0.8
SAVE_EVERY = 10

RETRY_SR = False
EARLY_STOP_SCORE = 92
DEBUG_LOG = True

# -------------------------- NUEVO: control de fallos --------------------------
MAX_CONSECUTIVE_ERRORS = 10
MAX_TOTAL_FETCH_ERRORS = 25


class FatalProcessError(Exception):
    pass


class ProcessState:
    def __init__(self):
        self.consecutive_errors = 0 
        self.total_fetch_errors = 0
        self.stop_requested = False
        self.stop_reason = ""

    def register_success(self):
        self.consecutive_errors = 0

    def register_error(self, reason: str):
        self.consecutive_errors += 1
        self.total_fetch_errors += 1

        if self._looks_like_api_exhausted(reason):
            self.stop_requested = True
            self.stop_reason = f"APIs agotadas o no disponibles: {reason}"
            raise FatalProcessError(self.stop_reason)

        if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            self.stop_requested = True
            self.stop_reason = f"Se alcanzó el máximo de errores consecutivos ({self.consecutive_errors}). Último error: {reason}"
            raise FatalProcessError(self.stop_reason)

        if self.total_fetch_errors >= MAX_TOTAL_FETCH_ERRORS:
            self.stop_requested = True
            self.stop_reason = f"Se alcanzó el máximo de errores acumulados ({self.total_fetch_errors}). Último error: {reason}"
            raise FatalProcessError(self.stop_reason)

    @staticmethod
    def _looks_like_api_exhausted(reason: str) -> bool:
        r = (reason or "").lower()
        patterns = [
            "429",
            "quota",
            "rate limit",
            "rate-limit",
            "too many requests",
            "api key",
            "api keys",
            "no api",
            "no keys",
            "sin api",
            "sin keys",
            "agotad",
            "exhaust",
            "limit exceeded",
            "insufficient quota",
            "forbidden",
            "unauthorized",
        ]
        return any(p in r for p in patterns)


def wait_api():
    time.sleep(random.uniform(DELAY_API_MIN, DELAY_API_MAX))


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


def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _jsonish(v):
    try:
        if isinstance(v, (list, dict, tuple, set)):
            return json.dumps(list(v) if isinstance(v, set) else v, ensure_ascii=False)
        return v
    except Exception:
        return str(v)


def resolve_col(row, *candidates: str) -> str:
    for col in candidates:
        val = safe_str(row.get(col))
        if val:
            return val
    return ""


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
    for col in ("Cedula", "Carnet"):
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def is_already_filled(v: str) -> bool:
    v = safe_str(v)
    return v.startswith("http") and "linkedin.com" in v.lower()


def append_debug_row(
    debug_rows: list,
    estudiante: str,
    carrera: str,
    anio_graduacion,
    universidad: str,
    query: str,
    variant: str,
    item: dict,
    score: int,
    flags: dict,
    debug: dict,
):
    row = {
        "estudiante": estudiante,
        "carrera": carrera,
        "anio_graduacion": anio_graduacion,
        "universidad": universidad,
        "query": query,
        "variant": variant,
        "url": item.get("url", "") if item else "",
        "title": item.get("title", "") if item else "",
        "snippet": item.get("snippet", "") if item else "",
        "score": score,
        "flag_udla": flags.get("udla") if flags else "",
        "flag_carrera": flags.get("carrera") if flags else "",
        "flag_nombre": flags.get("nombre") if flags else "",
        "flag_anio": flags.get("anio") if flags else "",
    }

    if debug:
        for k, v in debug.items():
            row[f"dbg_{k}"] = _jsonish(v)

    debug_rows.append(row)


def save_debug_csv(debug_rows: list, debug_csv_path: str):
    if not debug_rows:
        return
    df_debug = pd.DataFrame(debug_rows)
    Path(debug_csv_path).parent.mkdir(parents=True, exist_ok=True)
    df_debug.to_csv(debug_csv_path, index=False, encoding="utf-8-sig")


def _extract_lote_num_from_name(path_str: str) -> str:
    name = Path(path_str).stem
    m = re.search(r"lote[_\- ]*(\d+)", name, flags=re.IGNORECASE)
    if m:
        return m.group(1).zfill(3)

    m2 = re.search(r"(\d+)", name)
    if m2:
        return m2.group(1).zfill(3)

    return "001"


def build_output_names(input_xlsx: str, pais: str, out_dir: Path):
    lote_num = _extract_lote_num_from_name(input_xlsx)
    excel_name = f"resultados_lote_{lote_num}_{pais}.xlsx"
    log_name = f"logs_lote_{lote_num}_{pais}.csv"
    return out_dir / excel_name, out_dir / log_name


def process_row(
    estudiante: str,
    carrera: str,
    key_manager,
    cache: dict,
    process_state: ProcessState,
    anio_graduacion=None,
    pais: str = "EC",
    universidad: str = "",
    debug_rows: list | None = None,
):
    _scoring = _scoring_cr if pais == "CR" else _scoring_ec

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

            try:
                wait_api()
                results = serper_search_with_rotation(
                    key_manager=key_manager,
                    query=q,
                    count=count,
                    max_retries=5,
                )

                if results is None:
                    raise RuntimeError("serper_search_with_rotation devolvió None")

                cache[ck] = results
                process_state.register_success()
                return results

            except FatalProcessError:
                raise
            except Exception as e:
                reason = str(e) or e.__class__.__name__
                process_state.register_error(reason)
                raise

        def evaluate(results, query_used: str, variant_used: str):
            nonlocal scored_all

            for it in results:
                if not is_profile_url(it.get("url", "")):
                    if DEBUG_LOG and debug_rows is not None:
                        append_debug_row(
                            debug_rows,
                            estudiante,
                            carrera,
                            anio_graduacion,
                            universidad,
                            query_used,
                            variant_used,
                            it,
                            0,
                            {"udla": False, "carrera": False, "nombre": False, "anio": None},
                            {"accepted": False, "stage": "profile_url", "reason": "url_no_es_perfil"},
                        )
                    continue

                if pais == "CR" and hasattr(_scoring, "score_candidate_debug"):
                    s, flags, debug = _scoring.score_candidate_debug(
                        carrera,
                        estudiante,
                        it,
                        anio_graduacion,
                        universidad=universidad,
                    )
                else:
                    s, flags, debug = _scoring.score_candidate(
                        carrera,
                        estudiante,
                        it,
                        anio_graduacion,
                        universidad=universidad,
                    )
                    if not isinstance(debug, dict):
                        debug = {}

                s = int(s)

                if DEBUG_LOG and debug_rows is not None:
                    append_debug_row(
                        debug_rows,
                        estudiante,
                        carrera,
                        anio_graduacion,
                        universidad,
                        query_used,
                        variant_used,
                        it,
                        s,
                        flags,
                        debug,
                    )

                if s <= 0:
                    continue

                nc = _scoring.name_closeness_for_item(estudiante, it)
                scored_all.append((s, nc, it, flags))

                if s >= EARLY_STOP_SCORE:
                    return True

            return False

        for v in variants:
            q1 = build_query(v, carrera, mode="name_udlaec", pais=pais, universidad=universidad)
            stop = evaluate(fetch_query(q1, 15), q1, v)
            if stop:
                break

            q2 = build_query(v, carrera, mode="strict", pais=pais, universidad=universidad, max_terms=4)
            stop = evaluate(fetch_query(q2, 10), q2, v)
            if stop:
                break

        if pais == "CR":
            for v in variants[:2]:
                qf = build_query(v, carrera, mode="name_country", pais=pais, universidad=universidad)
                stop = evaluate(fetch_query(qf, 15), qf, v)
                if stop:
                    break

        if not scored_all:
            return empty_result()

        def _norm_url_local(u: str) -> str:
            return (u or "").split("?")[0].rstrip("/").lower()

        dedup = {}
        for s, nc, it, flags in scored_all:
            nu = _norm_url_local(it.get("url", ""))
            if not nu:
                continue
            prev = dedup.get(nu)
            if prev is None or (s, nc) > (prev[0], prev[1]):
                dedup[nu] = (s, nc, it, flags)

        scored_all = list(dedup.values())

        top3_by_score = sorted(scored_all, key=lambda x: (x[0], x[1]), reverse=True)[:3]
        best = sorted(scored_all, key=lambda x: (x[1], x[0]), reverse=True)[0]
        best_score, _, best_item, best_flags = best

        if pais == "CR":
            thr_alta, thr_revisar = 85, 70
        else:
            thr_alta, thr_revisar = 85, 65

        if best_score >= thr_alta and best_flags.get("carrera") and best_flags.get("udla"):
            conf = "ALTA"
        elif best_score >= thr_revisar:
            conf = "REVISAR"
        else:
            conf = "BAJA"

        return build_output(best_score, best_item, best_flags, top3_by_score, conf)

    except FatalProcessError:
        raise
    except Exception:
        traceback.print_exc()
        if DEBUG_LOG and debug_rows is not None:
            debug_rows.append({
                "estudiante": estudiante,
                "carrera": carrera,
                "anio_graduacion": anio_graduacion,
                "universidad": universidad,
                "dbg_accepted": False,
                "dbg_stage": "exception",
                "dbg_reason": traceback.format_exc(),
            })
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

    best_url = (best_item.get("url", "") or "") if conf in ("ALTA", "REVISAR") else ""

    anio_status = best_flags.get("anio")
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


def run_lote(
    input_xlsx,
    output_xlsx,
    key_manager,
    cache,
    cache_path,
    process_state: ProcessState,
    pais: str = "EC",
    debug_csv_path: str | None = None,
):
    if Path(output_xlsx).exists():
        print("Reanudando desde archivo existente...")
        df = pd.read_excel(output_xlsx)
    else:
        df = pd.read_excel(input_xlsx)
        df = ensure_columns(df)
        df.to_excel(output_xlsx, index=False)

    df = ensure_columns(df)

    if debug_csv_path is None:
        output_path = Path(output_xlsx)
        debug_csv_path = str(output_path.with_name(f"{output_path.stem}_debug.csv"))

    debug_rows = []

    def _norm_url(u: str) -> str:
        return (u or "").split("?")[0].rstrip("/").lower()

    assigned_urls: set[str] = set()
    for _, row in df.iterrows():
        v = safe_str(row.get("LinkedIn"))
        if is_already_filled(v):
            assigned_urls.add(_norm_url(v))

    try:
        for i, row in df.iterrows():
            if process_state.stop_requested:
                raise FatalProcessError(process_state.stop_reason or "Stop solicitado")

            linkedin_val = safe_str(row.get("LinkedIn"))
            score_val = int(row.get("Score") or 0)
            conf_val = safe_str(row.get("Confianza"))

            if is_already_filled(linkedin_val) and score_val > 0 and conf_val:
                continue

            if (not RETRY_SR) and (linkedin_val == "SR"):
                continue

            estudiante = resolve_col(row, "nombre", "Estudiante", "NOMBRE")
            carrera = resolve_col(row, "carrera", "Carrera", "CARRERA")

            if not estudiante or not carrera:
                continue

            anio_graduacion = resolve_col(row, "anio_graduacion", "Anio_Graduacion", "año_graduacion")
            universidad = resolve_col(row, "universidad", "Universidad") if pais == "CR" else ""

            out = process_row(
                estudiante,
                carrera,
                key_manager,
                cache,
                process_state,
                anio_graduacion,
                pais=pais,
                universidad=universidad,
                debug_rows=debug_rows,
            )

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
                if DEBUG_LOG:
                    save_debug_csv(debug_rows, debug_csv_path)

            time.sleep(DELAY_ROW)

    except FatalProcessError as e:
        print(f"🛑 Proceso detenido: {e}")

    finally:
        df.to_excel(output_xlsx, index=False)
        save_cache(cache, cache_path)
        if DEBUG_LOG:
            save_debug_csv(debug_rows, debug_csv_path)

        print("✅ Guardado final realizado.")
        if DEBUG_LOG:
            print(f"🧪 Log CSV generado en: {debug_csv_path}")


PAISES = {
    "1": {"codigo": "EC", "nombre": "Ecuador", "carpeta": "Ecuador"},
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
    process_state = ProcessState()

    base_dir = f"data/{carpeta}"
    in_dir = Path(f"{base_dir}/lotes")
    out_dir = Path(f"{base_dir}/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    generados = procesar_dividir(base_dir)
    if generados:
        print(f"[dividir] {generados} lote(s) generado(s) desde carpeta 'dividir'")

    cache_path = f"cache/serper_cache_{pais.lower()}.json"
    cache = load_cache(cache_path)

    lotes = sorted(in_dir.glob("*.xlsx"))
    if not lotes:
        raise RuntimeError(f"No se encontraron lotes en: {in_dir.resolve()}")

    for f in lotes:
        if process_state.stop_requested:
            print(f"🛑 Se detiene antes de iniciar otro lote: {process_state.stop_reason}")
            break

        out_file, log_file = build_output_names(str(f), pais, out_dir)
        print(f"[RUN] {f.name} -> {out_file.name}")

        run_lote(
            input_xlsx=str(f),
            output_xlsx=str(out_file),
            key_manager=key_manager,
            cache=cache,
            cache_path=cache_path,
            process_state=process_state,
            pais=pais,
            debug_csv_path=str(log_file),
        )

    print("Listo. Proceso finalizado.")


if __name__ == "__main__":
    main()