from __future__ import annotations

import re

from utils.text_norm import norm, token_set
from utils.nombres import unir_particulas_apellido

from utils.scoring import (
    extract_years,
    graduation_year_score,
    is_profile_url,
    extract_slug,
    COMMON_GIVEN,
    _ap_in_slug,
    fuzzy_ratio,
    career_score,
    infer_study_status,
)


# -------------------------------------------------
# DATASET PARSING — CR: NOMBRE1 NOMBRE2 APELLIDO1 APELLIDO2
# -------------------------------------------------

def split_nombre_cr(nombre: str):
    parts = [p for p in norm(nombre).split() if len(p) >= 2]
    if len(parts) < 2:
        return [], None, None

    if len(parts) == 2:
        return [parts[0]], parts[1], ""

    ap2 = parts[-1]
    ap1 = parts[-2]
    nombres = parts[:-2]

    return nombres, ap1, ap2


# -------------------------------------------------
# VALIDACIÓN UNIVERSIDAD (CR)
# -------------------------------------------------

_EDUCACION_TOKENS = {
    "educacion", "education", "preescolar", "preschool",
    "primaria", "secundaria", "docencia", "pedagogia",
    "pedagogy", "teaching", "enseñanza", "ensenanza",
    "ciencias de la educacion",
}


def _is_educacion(carrera: str) -> bool:
    t = norm(carrera)
    return any(kw in t for kw in _EDUCACION_TOKENS)


def _universidad_required(text: str, universidad: str = "", strict: bool = False) -> bool:
    from utils.pais_config import validate_universidad
    return validate_universidad(norm(text), "CR", universidad, strict=strict)


# -------------------------------------------------
# Helpers de nombre / slug
# -------------------------------------------------

_GENERIC_NAME_LASTNAME_BLOCKLIST = {
    ("mario", "cespedes"),
    ("adrian", "fallas"),
    ("jorge", "martinez"),
    ("guillermo", "martinez"),
    ("lorena", "vallejos"),
    ("jorge", "gomez"),
    ("jose", "garcia"),
}


def _clean_slug_tokens(url: str) -> list[str]:
    """
    Corrige el bug principal:
    extract_slug(url) a veces devuelve:
        "patricia cordero 194402287"
    en vez de:
        "patricia-cordero-194402287"

    Entonces ya NO se puede hacer split("-").
    Aquí tokenizamos usando regex sobre el slug normalizado.
    """
    slug = norm(extract_slug(url))

    if not slug:
        return []

    raw_tokens = re.findall(r"[a-záéíóúñü]+", slug, flags=re.IGNORECASE)
    toks = []

    for t in raw_tokens:
        t = norm(t)
        if not t:
            continue
        if len(t) < 3:
            continue
        toks.append(t)

    return toks


def _valid_name_tokens(estudiante: str):
    nombres, ap1, ap2 = split_nombre_cr(estudiante)
    nombres_validos = [n for n in nombres if n and len(n) >= 3]
    apellidos_validos = [a for a in [ap1, ap2] if a and len(a) >= 3]
    return nombres_validos, apellidos_validos, ap1, ap2


def _debug_base(url: str, estudiante: str, carrera: str, universidad: str = "") -> dict:
    nombres_validos, apellidos_validos, ap1, ap2 = _valid_name_tokens(estudiante)
    slug = extract_slug(url)
    slug_tokens = _clean_slug_tokens(url)
    return {
        "url": url,
        "slug": slug,
        "slug_tokens": slug_tokens,
        "estudiante": estudiante,
        "carrera": carrera,
        "universidad": universidad,
        "nombres_validos": nombres_validos,
        "apellidos_validos": apellidos_validos,
        "ap1": ap1 or "",
        "ap2": ap2 or "",
    }


# -------------------------------------------------
# SLUG GATING — CR
# -------------------------------------------------

def slug_gating_pass(estudiante: str, url: str, title_snippet_text: str) -> bool:
    slug_tokens = _clean_slug_tokens(url)
    nombres_validos, apellidos_validos, ap1, ap2 = _valid_name_tokens(estudiante)

    if not slug_tokens:
        return False

    matched_names = [t for t in slug_tokens if t in nombres_validos]
    matched_lastnames = [t for t in slug_tokens if t in apellidos_validos]

    if not matched_names or not matched_lastnames:
        return False

    for ap in set(apellidos_validos):
        if slug_tokens.count(ap) > 1:
            real_count = apellidos_validos.count(ap)
            if real_count < slug_tokens.count(ap):
                return False

    if len(slug_tokens) == 2:
        nombre_tok, apellido_tok = slug_tokens[0], slug_tokens[1]
        if nombre_tok in nombres_validos and apellido_tok in apellidos_validos:
            if (nombre_tok, apellido_tok) in _GENERIC_NAME_LASTNAME_BLOCKLIST:
                return False
            return True
        return False

    total_matches = len(set(matched_names + matched_lastnames))
    return total_matches >= 2


def slug_gating_pass_debug(estudiante: str, url: str, title_snippet_text: str):
    d = _debug_base(url, estudiante, "")
    slug_tokens = d["slug_tokens"]
    nombres_validos = d["nombres_validos"]
    apellidos_validos = d["apellidos_validos"]

    if not slug_tokens:
        return False, {
            **d,
            "gate": "slug_gating_pass",
            "ok": False,
            "reason": "slug_sin_tokens_validos",
        }

    matched_names = [t for t in slug_tokens if t in nombres_validos]
    matched_lastnames = [t for t in slug_tokens if t in apellidos_validos]

    if not matched_names:
        return False, {
            **d,
            "gate": "slug_gating_pass",
            "ok": False,
            "reason": "sin_nombre_en_slug",
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    if not matched_lastnames:
        return False, {
            **d,
            "gate": "slug_gating_pass",
            "ok": False,
            "reason": "sin_apellido_en_slug",
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    for ap in set(apellidos_validos):
        if slug_tokens.count(ap) > 1:
            real_count = apellidos_validos.count(ap)
            if real_count < slug_tokens.count(ap):
                return False, {
                    **d,
                    "gate": "slug_gating_pass",
                    "ok": False,
                    "reason": "apellido_repetido_sospechoso",
                    "matched_names": matched_names,
                    "matched_lastnames": matched_lastnames,
                }

    if len(slug_tokens) == 2:
        nombre_tok, apellido_tok = slug_tokens[0], slug_tokens[1]
        if nombre_tok in nombres_validos and apellido_tok in apellidos_validos:
            if (nombre_tok, apellido_tok) in _GENERIC_NAME_LASTNAME_BLOCKLIST:
                return False, {
                    **d,
                    "gate": "slug_gating_pass",
                    "ok": False,
                    "reason": "combo_generico_bloqueado",
                    "matched_names": matched_names,
                    "matched_lastnames": matched_lastnames,
                }
            return True, {
                **d,
                "gate": "slug_gating_pass",
                "ok": True,
                "reason": "slug_2_tokens_valido",
                "matched_names": matched_names,
                "matched_lastnames": matched_lastnames,
            }
        return False, {
            **d,
            "gate": "slug_gating_pass",
            "ok": False,
            "reason": "slug_2_tokens_no_valido",
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    total_matches = len(set(matched_names + matched_lastnames))
    ok = total_matches >= 2
    return ok, {
        **d,
        "gate": "slug_gating_pass",
        "ok": ok,
        "reason": "slug_match_suficiente" if ok else "slug_match_insuficiente",
        "matched_names": matched_names,
        "matched_lastnames": matched_lastnames,
        "total_matches": total_matches,
    }


# -------------------------------------------------
# VALIDACIÓN FUERTE
# -------------------------------------------------

def strict_name_tokens_match(estudiante: str, url: str) -> bool:
    slug_tokens = _clean_slug_tokens(url)
    nombres_validos, apellidos_validos, ap1, ap2 = _valid_name_tokens(estudiante)
    valid_tokens = set(nombres_validos + apellidos_validos)

    if not slug_tokens:
        return False

    matches = sum(1 for tok in slug_tokens if tok in valid_tokens)
    matched_names = sum(1 for tok in slug_tokens if tok in nombres_validos)
    matched_lastnames = sum(1 for tok in slug_tokens if tok in apellidos_validos)

    if matched_names == 0 or matched_lastnames == 0:
        return False

    if len(slug_tokens) == 1:
        return False

    if len(slug_tokens) == 2:
        if matches != 2:
            return False
        if tuple(slug_tokens) in _GENERIC_NAME_LASTNAME_BLOCKLIST:
            return False
        return True

    if len(slug_tokens) >= 3:
        return matches >= (len(slug_tokens) - 1)

    return False


def strict_name_tokens_match_debug(estudiante: str, url: str):
    d = _debug_base(url, estudiante, "")
    slug_tokens = d["slug_tokens"]
    nombres_validos = d["nombres_validos"]
    apellidos_validos = d["apellidos_validos"]
    valid_tokens = set(nombres_validos + apellidos_validos)

    if not slug_tokens:
        return False, {
            **d,
            "gate": "strict_name_tokens_match",
            "ok": False,
            "reason": "slug_sin_tokens_validos",
        }

    matches = sum(1 for tok in slug_tokens if tok in valid_tokens)
    matched_names = sum(1 for tok in slug_tokens if tok in nombres_validos)
    matched_lastnames = sum(1 for tok in slug_tokens if tok in apellidos_validos)

    if matched_names == 0:
        return False, {
            **d,
            "gate": "strict_name_tokens_match",
            "ok": False,
            "reason": "sin_nombre_valido",
            "matches": matches,
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    if matched_lastnames == 0:
        return False, {
            **d,
            "gate": "strict_name_tokens_match",
            "ok": False,
            "reason": "sin_apellido_valido",
            "matches": matches,
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    if len(slug_tokens) == 1:
        return False, {
            **d,
            "gate": "strict_name_tokens_match",
            "ok": False,
            "reason": "slug_demasiado_corto",
            "matches": matches,
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    if len(slug_tokens) == 2:
        if matches != 2:
            return False, {
                **d,
                "gate": "strict_name_tokens_match",
                "ok": False,
                "reason": "slug_2_tokens_match_incompleto",
                "matches": matches,
                "matched_names": matched_names,
                "matched_lastnames": matched_lastnames,
            }
        if tuple(slug_tokens) in _GENERIC_NAME_LASTNAME_BLOCKLIST:
            return False, {
                **d,
                "gate": "strict_name_tokens_match",
                "ok": False,
                "reason": "slug_2_tokens_generico",
                "matches": matches,
                "matched_names": matched_names,
                "matched_lastnames": matched_lastnames,
            }
        return True, {
            **d,
            "gate": "strict_name_tokens_match",
            "ok": True,
            "reason": "slug_2_tokens_ok",
            "matches": matches,
            "matched_names": matched_names,
            "matched_lastnames": matched_lastnames,
        }

    ok = matches >= (len(slug_tokens) - 1)
    return ok, {
        **d,
        "gate": "strict_name_tokens_match",
        "ok": ok,
        "reason": "match_suficiente" if ok else "demasiados_tokens_no_coinciden",
        "matches": matches,
        "matched_names": matched_names,
        "matched_lastnames": matched_lastnames,
        "slug_len": len(slug_tokens),
    }


# -------------------------------------------------
# Name closeness — CR
# -------------------------------------------------

def name_closeness_for_item(estudiante: str, item: dict) -> int:
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    slug = extract_slug(url)
    slug_tokens = token_set(slug)
    txt_tokens = token_set(f"{title} {snippet}")

    nombres, ap1, ap2 = split_nombre_cr(estudiante)

    score = 0

    if _ap_in_slug(ap1, slug, slug_tokens):
        score += 60
    if _ap_in_slug(ap2, slug, slug_tokens):
        score += 45

    for n in nombres:
        if n in slug_tokens or n in txt_tokens:
            score += 18 if n not in COMMON_GIVEN else 6
        else:
            score -= 8

    score += int(fuzzy_ratio(estudiante, f"{slug} {title}") * 0.1)

    return score


# -------------------------------------------------
# MAIN SCORING — CR
# -------------------------------------------------

def score_candidate(carrera: str, estudiante: str, item: dict, anio_graduacion=None, universidad: str = ""):
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    if not is_profile_url(url):
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {}

    slug = extract_slug(url)
    text = f"{title} {snippet} {slug}"
    t = norm(text)

    if not slug_gating_pass(estudiante, url, f"{title} {snippet}"):
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {}

    if not strict_name_tokens_match(estudiante, url):
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {}

    strict_univ = _is_educacion(carrera)
    if not _universidad_required(t, universidad, strict=strict_univ):
        return 0, {"udla": False, "carrera": False, "nombre": True, "anio": None}, {}

    nombres, ap1, ap2 = split_nombre_cr(estudiante)
    slug_tokens = token_set(slug)

    ap1_in_slug = _ap_in_slug(ap1, slug, slug_tokens)
    ap2_in_slug = _ap_in_slug(ap2, slug, slug_tokens)

    score = 45

    cs = career_score(t, carrera)
    score += cs

    if ap1_in_slug:
        score += 25
    if ap2_in_slug:
        score += 20

    for n in nombres:
        if n and n in slug_tokens:
            score += 10

    if ap1_in_slug and ap2_in_slug:
        score += 8

    sim2 = fuzzy_ratio(estudiante, f"{slug} {title}")
    if sim2 >= 95:
        score += 10
    elif sim2 >= 88:
        score += 5

    anio_delta, anio_status = graduation_year_score(t, anio_graduacion)
    score += anio_delta

    score = max(0, min(100, score))

    flags = {
        "udla": True,
        "carrera": cs > 0,
        "nombre": True,
        "anio": anio_status,
    }

    return score, flags, {}


def score_candidate_debug(carrera: str, estudiante: str, item: dict, anio_graduacion=None, universidad: str = ""):
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    base_debug = {
        "url": url,
        "title": title,
        "snippet": snippet,
        "estudiante": estudiante,
        "carrera": carrera,
        "universidad": universidad,
    }

    if not is_profile_url(url):
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {
            **base_debug,
            "accepted": False,
            "stage": "profile_url",
            "reason": "url_no_es_perfil",
        }

    slug = extract_slug(url)
    text = f"{title} {snippet} {slug}"
    t = norm(text)

    ok1, dbg1 = slug_gating_pass_debug(estudiante, url, f"{title} {snippet}")
    if not ok1:
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {
            **base_debug,
            **dbg1,
            "accepted": False,
            "stage": "gate_slug",
        }

    ok2, dbg2 = strict_name_tokens_match_debug(estudiante, url)
    if not ok2:
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {
            **base_debug,
            **dbg2,
            "accepted": False,
            "stage": "gate_strict_name",
        }

    strict_univ = _is_educacion(carrera)
    univ_ok = _universidad_required(t, universidad, strict=strict_univ)
    if not univ_ok:
        return 0, {"udla": False, "carrera": False, "nombre": True, "anio": None}, {
            **base_debug,
            "accepted": False,
            "stage": "gate_universidad",
            "reason": "universidad_no_coincide",
            "strict_univ": strict_univ,
            "slug": slug,
            "text_norm": t,
        }

    nombres, ap1, ap2 = split_nombre_cr(estudiante)
    slug_tokens = token_set(slug)

    ap1_in_slug = _ap_in_slug(ap1, slug, slug_tokens)
    ap2_in_slug = _ap_in_slug(ap2, slug, slug_tokens)

    score = 45

    cs = career_score(t, carrera)
    score += cs

    if ap1_in_slug:
        score += 25
    if ap2_in_slug:
        score += 20

    nombres_en_slug = []
    for n in nombres:
        if n and n in slug_tokens:
            score += 10
            nombres_en_slug.append(n)

    if ap1_in_slug and ap2_in_slug:
        score += 8

    sim2 = fuzzy_ratio(estudiante, f"{slug} {title}")
    if sim2 >= 95:
        score += 10
    elif sim2 >= 88:
        score += 5

    anio_delta, anio_status = graduation_year_score(t, anio_graduacion)
    score += anio_delta

    score = max(0, min(100, score))

    flags = {
        "udla": True,
        "carrera": cs > 0,
        "nombre": True,
        "anio": anio_status,
    }

    debug = {
        **base_debug,
        "accepted": True,
        "stage": "accepted",
        "reason": "candidato_aceptado",
        "slug": slug,
        "slug_tokens": list(slug_tokens),
        "ap1_in_slug": ap1_in_slug,
        "ap2_in_slug": ap2_in_slug,
        "nombres_en_slug": nombres_en_slug,
        "career_score": cs,
        "fuzzy_ratio": sim2,
        "anio_delta": anio_delta,
        "anio_status": anio_status,
        "final_score": score,
        "strict_univ": strict_univ,
    }

    return score, flags, debug