"""
test_row.py — Prueba filas específicas del pipeline de búsqueda.

Uso:
    python test_row.py                  # menú interactivo
    python test_row.py --pais CR --fila 5
    python test_row.py --pais EC --fila 12 --archivo estudiantes_lote_001.xlsx
    python test_row.py --pais CR --nombre "PATRICIA CORDERO QUIROS" --carrera "Administracion" --universidad "Universidad Latina de Costa Rica"
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from utils.serper_client import SerperKeyManager, serper_search_with_rotation
from utils.scoring import (
    is_profile_url,
    extract_slug,
    _ap_in_slug,
    infer_study_status,
)
import utils.scoring as _scoring_ec
import utils.scoring_cr as _scoring_cr
from utils.text_norm import token_set as _token_set
from utils.pais_config import build_query, validate_universidad
from utils.text_norm import norm
from utils.nombres import variantes_nombres, variantes_nombres_cr
from utils.carreras_synonyms import expand_carrera_keywords


def _get_scoring(pais: str):
    return _scoring_cr if pais == "CR" else _scoring_ec


def split_by_pais(nombre: str, pais: str):
    if pais == "CR":
        return _scoring_cr.split_nombre_cr(nombre)
    return _scoring_ec.split_nombre_dataset(nombre)


# ── Constantes ────────────────────────────────────────────────────────────────

PAISES = {
    "1": {"codigo": "EC", "nombre": "Ecuador",    "carpeta": "Ecuador"},
    "2": {"codigo": "CR", "nombre": "Costa Rica", "carpeta": "Costa_Rica"},
}

CACHE_PATH = {
    "EC": "cache/serper_cache_ec.json",
    "CR": "cache/serper_cache_cr.json",
}


# ── Helpers de I/O ────────────────────────────────────────────────────────────

def sep(char="─", n=72):
    print(char * n)

def titulo(texto):
    sep("═")
    print(f"  {texto}")
    sep("═")

def seccion(texto):
    sep()
    print(f"  {texto}")
    sep()

def safe_str(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


# ── Cache ─────────────────────────────────────────────────────────────────────

def load_cache(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict, path: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Selección interactiva ─────────────────────────────────────────────────────

def seleccionar_pais() -> dict:
    print("\nSelecciona el país:")
    for k, v in PAISES.items():
        print(f"  {k}. {v['nombre']} ({v['codigo']})")
    while True:
        op = input("Opción [1/2]: ").strip()
        if op in PAISES:
            return PAISES[op]
        print("Opción inválida.")


def seleccionar_archivo(carpeta: str) -> Path | None:
    lotes_dir = Path(f"data/{carpeta}/lotes")
    archivos = sorted(lotes_dir.glob("*.xlsx"))
    if not archivos:
        print(f"No hay archivos en {lotes_dir}")
        return None
    if len(archivos) == 1:
        print(f"Archivo: {archivos[0].name}")
        return archivos[0]
    print("\nArchivos disponibles:")
    for i, f in enumerate(archivos, 1):
        print(f"  {i}. {f.name}")
    while True:
        op = input(f"Número de archivo [1-{len(archivos)}]: ").strip()
        if op.isdigit() and 1 <= int(op) <= len(archivos):
            return archivos[int(op) - 1]
        print("Opción inválida.")


def seleccionar_filas(df: pd.DataFrame) -> list[int]:
    print(f"\nEl archivo tiene {len(df)} filas (índice 0–{len(df)-1}).")
    print("Ingresa los números de fila separados por coma. Ej: 0,5,12")
    while True:
        entrada = input("Filas: ").strip()
        try:
            filas = [int(x.strip()) for x in entrada.split(",") if x.strip()]
            invalidas = [f for f in filas if f < 0 or f >= len(df)]
            if invalidas:
                print(f"Fuera de rango: {invalidas}")
                continue
            return filas
        except ValueError:
            print("Formato inválido.")


# ── Diagnóstico detallado de un candidato ────────────────────────────────────

def _gate_nombre_detalle(estudiante: str, url: str, title: str, snippet: str, pais: str) -> str:
    """Retorna string explicando qué check del gate de nombre pasa o falla."""
    slug        = extract_slug(url)
    slug_tokens = _token_set(slug)
    txt_tokens  = _token_set(f"{title} {snippet}")
    nombres, ap1, ap2 = split_by_pais(estudiante, pais)

    ap1_ok = _ap_in_slug(ap1, slug, slug_tokens)
    ap2_ok = _ap_in_slug(ap2, slug, slug_tokens) if ap2 else None

    if pais == "CR":
        nom_ok = any(n and n in slug_tokens for n in nombres)
        partes = [
            f"ap1('{ap1}') en slug: {'✓' if ap1_ok else '✗'}",
            f"nombre en slug: {'✓' if nom_ok else '✗'}",
            f"ap2('{ap2}') en slug: {'✓' if ap2_ok else '✗ (bonus, no requerido)'}",
        ]
    else:
        nom_ok = any(n and (n in slug_tokens or n in txt_tokens) for n in nombres)
        partes = [
            f"ap1/ap2 en slug: {'✓' if (ap1_ok or ap2_ok) else '✗'}",
            f"nombre en slug/texto: {'✓' if nom_ok else '✗'}",
        ]
    return "  |  ".join(partes)


def diagnosticar_candidato(estudiante: str, carrera: str, item: dict,
                            pais: str, universidad: str, anio: str):
    url     = item.get("url", "")
    title   = item.get("title", "")
    snippet = item.get("snippet", "")
    slug    = extract_slug(url)

    _sc         = _get_scoring(pais)
    gate_nombre = _sc.slug_gating_pass(estudiante, url, f"{title} {snippet}")
    t           = norm(f"{title} {snippet} {slug}")
    gate_univ   = validate_universidad(t, pais, universidad)

    score, flags, _ = _sc.score_candidate(
        carrera, estudiante, item, anio, universidad=universidad
    )
    nc    = _sc.name_closeness_for_item(estudiante, item)
    study = infer_study_status(f"{title} {snippet}")

    nombres, ap1, ap2 = split_by_pais(estudiante, pais)

    print(f"  URL     : {url}")
    print(f"  Título  : {title}")
    print(f"  Snippet : {snippet[:120]}")
    print(f"  Slug    : {slug}")
    print()
    print(f"  Parseo  : nombres={nombres}  ap1='{ap1}'  ap2='{ap2}'")
    print(f"  Gate nombre  : {'✓ PASA' if gate_nombre else '✗ FALLA'}  →  {_gate_nombre_detalle(estudiante, url, title, snippet, pais)}")
    print(f"  Gate univ    : {'✓ PASA' if gate_univ   else '✗ FALLA'}")
    print(f"  Score        : {int(score)}")
    print(f"  Name close.  : {nc}")
    print(f"  Flags        : {flags}")
    print(f"  Estudia/ó    : {study}")


# ── Motor de prueba ───────────────────────────────────────────────────────────

def probar_fila(estudiante: str, carrera: str, universidad: str,
                anio: str, pais: str, key_manager, cache: dict,
                cache_path: str, verbose: bool = True):

    titulo(f"TEST | {pais} | {estudiante}")
    print(f"  Carrera     : {carrera}")
    if universidad:
        print(f"  Universidad : {universidad}")
    if anio:
        print(f"  Año grad.   : {anio}")
    print()

    # Variantes de nombre
    _gen = variantes_nombres_cr if pais == "CR" else variantes_nombres
    variants = _gen(estudiante, max_variantes=5)
    print(f"  Variantes de nombre : {variants}")

    # Keywords de carrera
    kws = expand_carrera_keywords(carrera)
    print(f"  Keywords carrera    : {kws[:8]}")
    print()

    queries_ejecutadas = []
    resultados_brutos: list[dict] = []   # {query, from_cache, items}
    scored_all: list[tuple] = []

    def fetch(q: str, count: int) -> list:
        ck = f"{count}::{q}"
        from_cache = ck in cache
        if not from_cache:
            time.sleep(1.5)
            items = serper_search_with_rotation(key_manager, q, count, max_retries=5)
            cache[ck] = items
        else:
            items = cache[ck]
        queries_ejecutadas.append(q)
        resultados_brutos.append({"query": q, "from_cache": from_cache, "items": items})
        return items

    _sc = _get_scoring(pais)

    def evaluar(results: list) -> bool:
        for it in results:
            if not is_profile_url(it.get("url", "")):
                continue
            s, flags, _ = _sc.score_candidate(
                carrera, estudiante, it, anio, universidad=universidad
            )
            s = int(s)
            nc = _sc.name_closeness_for_item(estudiante, it)
            scored_all.append((s, nc, it, flags))
            if s >= 92:
                return True
        return False

    EARLY_STOP = 92
    thr_alta, thr_revisar = (90, 75) if pais == "CR" else (85, 65)

    for v in variants:
        q1 = build_query(v, carrera, mode="name_udlaec", pais=pais, universidad=universidad)
        stop = evaluar(fetch(q1, 15))
        if stop:
            break
        q2 = build_query(v, carrera, mode="strict", pais=pais, universidad=universidad, max_terms=4)
        stop = evaluar(fetch(q2, 10))
        if stop:
            break

    # Fase 3 — fallback CR sin universidad
    if not scored_all and pais == "CR":
        for v in variants[:2]:
            qf = build_query(v, carrera, mode="name_country", pais=pais, universidad=universidad)
            stop = evaluar(fetch(qf, 15))
            if stop:
                break

    # ── Mostrar queries ──────────────────────────────────────────────────────
    seccion(f"QUERIES EJECUTADAS ({len(queries_ejecutadas)})")
    for i, r in enumerate(resultados_brutos, 1):
        cache_tag = "[CACHE]" if r["from_cache"] else "[API]  "
        print(f"  Q{i} {cache_tag} {r['query']}")
        perfiles = [x for x in r["items"] if is_profile_url(x.get("url",""))]
        print(f"       → {len(r['items'])} resultados, {len(perfiles)} perfiles LinkedIn")

    # ── Mostrar candidatos con score > 0 ────────────────────────────────────
    positivos = [(s, nc, it, fl) for s, nc, it, fl in scored_all if s > 0]
    positivos_sorted = sorted(positivos, key=lambda x: (x[0], x[1]), reverse=True)

    seccion(f"CANDIDATOS CON SCORE > 0  ({len(positivos_sorted)} encontrados)")

    if not positivos_sorted:
        # Mostrar por qué fallan los resultados LinkedIn encontrados
        todos_linkedin = []
        for r in resultados_brutos:
            for it in r["items"]:
                if is_profile_url(it.get("url", "")):
                    todos_linkedin.append(it)

        if todos_linkedin:
            print(f"  Se encontraron {len(todos_linkedin)} perfiles LinkedIn pero todos fallaron los gates.\n")
            for it in todos_linkedin[:8]:
                url     = it.get("url", "")
                title   = it.get("title", "")
                snippet = it.get("snippet", "")
                slug    = extract_slug(url)
                t       = norm(f"{title} {snippet} {slug}")
                g_nom   = _sc.slug_gating_pass(estudiante, url, f"{title} {snippet}")
                g_univ  = validate_universidad(t, pais, universidad)
                detalle = _gate_nombre_detalle(estudiante, url, title, snippet, pais)
                print(f"  {url}")
                print(f"    Slug        : {slug}")
                print(f"    Gate nombre : {'✓' if g_nom  else '✗ FALLA'}  →  {detalle}")
                print(f"    Gate univ   : {'✓' if g_univ else '✗ FALLA'}")
                print()
        else:
            print("  No se encontró ningún perfil LinkedIn en los resultados.")
    else:
        for rank, (s, nc, it, fl) in enumerate(positivos_sorted[:5], 1):
            print(f"\n  [#{rank}]  score={s}  closeness={nc}")
            diagnosticar_candidato(estudiante, carrera, it, pais, universidad, anio)

    # ── Resultado final ──────────────────────────────────────────────────────
    seccion("RESULTADO FINAL")
    if not scored_all or not positivos_sorted:
        print("  → NO ENCONTRADO")
    else:
        best_score, _, best_item, best_flags = positivos_sorted[0]
        if best_score >= thr_alta and best_flags.get("carrera") and best_flags.get("udla"):
            conf = "ALTA"
        elif best_score >= thr_revisar:
            conf = "REVISAR"
        else:
            conf = "BAJA"

        print(f"  URL        : {best_item.get('url','')}")
        print(f"  Score      : {best_score}")
        print(f"  Confianza  : {conf}  (umbrales: ALTA>={thr_alta}, REVISAR>={thr_revisar})")
        print(f"  Flags      : {best_flags}")

    save_cache(cache, cache_path)
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Prueba filas del pipeline de búsqueda")
    parser.add_argument("--pais",       choices=["EC", "CR"])
    parser.add_argument("--archivo",    help="Nombre del archivo en lotes/ (sin ruta)")
    parser.add_argument("--fila",       type=int, help="Índice de fila (0-based)")
    parser.add_argument("--nombre",     help="Nombre del estudiante (modo manual)")
    parser.add_argument("--carrera",    help="Carrera (modo manual)")
    parser.add_argument("--universidad",help="Universidad (modo manual, solo CR)")
    parser.add_argument("--anio",       help="Año de graduación (modo manual)")
    args = parser.parse_args()

    key_manager = SerperKeyManager.from_env()

    # ── Modo manual (sin Excel) ──────────────────────────────────────────────
    if args.nombre and args.carrera:
        pais = args.pais or input("País [EC/CR]: ").strip().upper()
        cache_path = CACHE_PATH[pais]
        cache = load_cache(cache_path)
        probar_fila(
            estudiante  = args.nombre,
            carrera     = args.carrera,
            universidad = args.universidad or "",
            anio        = args.anio or "",
            pais        = pais,
            key_manager = key_manager,
            cache       = cache,
            cache_path  = cache_path,
        )
        return

    # ── Modo Excel ───────────────────────────────────────────────────────────
    pais_info = None
    for v in PAISES.values():
        if v["codigo"] == args.pais:
            pais_info = v
    if not pais_info:
        pais_info = seleccionar_pais()

    pais    = pais_info["codigo"]
    carpeta = pais_info["carpeta"]
    cache_path = CACHE_PATH[pais]
    cache = load_cache(cache_path)

    # Selección de archivo
    if args.archivo:
        archivo = Path(f"data/{carpeta}/lotes/{args.archivo}")
    else:
        archivo = seleccionar_archivo(carpeta)
    if not archivo or not archivo.exists():
        print(f"Archivo no encontrado: {archivo}")
        sys.exit(1)

    df = pd.read_excel(archivo, dtype=str)
    df = df.fillna("")
    print(f"\nArchivo cargado: {archivo.name}  ({len(df)} filas)")
    print(f"Columnas: {list(df.columns)}")

    # Preview de las primeras filas
    print()
    print(df.head(10).to_string())
    print()

    # Selección de filas
    if args.fila is not None:
        filas = [args.fila]
    else:
        filas = seleccionar_filas(df)

    for idx in filas:
        row = df.iloc[idx]

        # Resolver columnas (acepta minúsculas o title-case)
        def rc(*cols):
            for c in cols:
                v = str(row.get(c, "")).strip()
                if v and v.lower() not in ("nan", "none", ""):
                    return v
            return ""

        estudiante  = rc("nombre", "Estudiante", "NOMBRE")
        carrera     = rc("carrera", "Carrera", "CARRERA")
        universidad = rc("universidad", "Universidad") if pais == "CR" else ""
        anio        = rc("anio_graduacion", "Anio_Graduacion", "año_graduacion")

        if not estudiante or not carrera:
            print(f"Fila {idx}: sin nombre o carrera, se omite.")
            continue

        probar_fila(
            estudiante  = estudiante,
            carrera     = carrera,
            universidad = universidad,
            anio        = anio,
            pais        = pais,
            key_manager = key_manager,
            cache       = cache,
            cache_path  = cache_path,
        )


if __name__ == "__main__":
    main()
