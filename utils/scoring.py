from __future__ import annotations

import re

from utils.text_norm import norm, simple_tokens, token_set
from utils.carreras_synonyms import expand_carrera_keywords, is_generic_kw
from utils.nombres import unir_particulas_apellido


# -------------------------------------------------
# AÑO DE GRADUACIÓN
# -------------------------------------------------

_YEAR_RE = re.compile(r'\b(19[9]\d|20[0-3]\d)\b')


def extract_years(text: str) -> set[int]:
    """Extrae años plausibles (1990-2039) del texto del perfil."""
    return {int(y) for y in _YEAR_RE.findall(norm(text))}


def graduation_year_score(text: str, expected_year) -> tuple[int, str | None]:
    """
    Compara el año de graduación esperado con los años del perfil.

    Returns (score_delta, status):
      (0, None)       → sin expected_year o sin años en perfil → neutral
      (+10, "MATCH")  → año coincide (tolerancia ±1)
      (-20, "NO COINCIDE") → años encontrados pero ninguno coincide
    """
    if not expected_year:
        return 0, None

    try:
        ey = int(str(expected_year).strip())
    except (ValueError, TypeError):
        return 0, None

    years = extract_years(text)
    if not years:
        return 0, None  # no hay info de año en el perfil → neutral

    if any(abs(y - ey) <= 1 for y in years):
        return 10, "MATCH"

    return -20, "NO COINCIDE"


# -------------------------------------------------
# LinkedIn helpers
# -------------------------------------------------

def is_profile_url(url: str) -> bool:
    u = (url or "").lower()
    return ("linkedin.com/in/" in u) and not any(
        x in u for x in ["/jobs", "/company", "/school", "/posts", "/pulse"]
    )


def extract_slug(url: str) -> str:
    try:
        from urllib.parse import unquote
        u = unquote(url or "").split("?")[0]  # decodifica %C3%B3 → ó, quita params
        slug = u.split("/in/")[1]
        slug = slug.split("/")[0]
        slug = slug.replace("-", " ")
        return norm(slug)                      # norm() quita tildes: ó → o
    except Exception:
        return ""


# -------------------------------------------------
# DATASET PARSING — EC: APELLIDO1 APELLIDO2 NOMBRE1 NOMBRE2
# -------------------------------------------------

CONNECTORS = {"de", "del", "la", "las", "los", "y", "san", "santa", "da", "do", "dos", "von", "van"}

COMMON_GIVEN = {
    "ana", "maria", "jose", "juan", "luis", "carlos",
    "pedro", "javier", "paola", "paula", "andres",
    "fernando", "daniel", "david", "ivan", "alex",
    "angel", "miguel", "sofia", "camila"
}


def split_nombre_dataset(nombre: str):
    """Formato EC: Ap1 Ap2 N1 N2"""
    parts = [p for p in norm(nombre).split() if len(p) >= 2]
    if len(parts) < 3:
        return [], None, None

    parts = unir_particulas_apellido(parts)

    ap1 = parts[0]
    ap2 = parts[1] if len(parts) >= 2 else ""
    nombres = parts[2:] if len(parts) >= 3 else []

    return nombres, ap1, ap2


def _ap_in_slug(ap: str, slug: str, slug_tokens: set) -> bool:
    """Verifica si un apellido (simple o compuesto) está en el slug."""
    if not ap:
        return False
    if " " in ap:  # apellido compuesto: buscar como frase exacta
        return ap in slug
    return ap in slug_tokens


# -------------------------------------------------
# FUZZY
# -------------------------------------------------

def fuzzy_ratio(a: str, b: str) -> int:
    aa = norm(a)
    bb = norm(b)
    if not aa or not bb:
        return 0

    try:
        from rapidfuzz import fuzz
        return int(fuzz.token_set_ratio(aa, bb))
    except Exception:
        sa = set(simple_tokens(aa))
        sb = set(simple_tokens(bb))
        if not sa or not sb:
            return 0
        return int((len(sa & sb) / len(sa | sb)) * 100)


# -------------------------------------------------
# SLUG GATING — EC
# -------------------------------------------------

def slug_gating_pass(estudiante: str, url: str, title_snippet_text: str) -> bool:
    """
    EC: apellido (ap1 o ap2) en slug + nombre en slug o texto.
    """
    slug = extract_slug(url)
    slug_tokens = token_set(slug)
    txt_tokens = token_set(title_snippet_text)

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)

    ap_ok = _ap_in_slug(ap1, slug, slug_tokens) or _ap_in_slug(ap2, slug, slug_tokens)
    if not ap_ok:
        return False

    nombre_ok = any(n and (n in slug_tokens or n in txt_tokens) for n in nombres)
    return nombre_ok


# -------------------------------------------------
# UDLA OBLIGATORIA (EC)
# -------------------------------------------------

def udla_ec_required(text: str, universidad: str = "", strict: bool = False) -> bool:
    from utils.pais_config import validate_universidad
    return validate_universidad(norm(text), "EC", universidad, strict=strict)


# -------------------------------------------------
# Career score
# -------------------------------------------------

def career_score(text: str, carrera: str) -> int:
    t = norm(text)
    tset = token_set(t)
    kws = expand_carrera_keywords(carrera)
    c_norm = norm(carrera)

    if c_norm and " " in c_norm and c_norm in t:
        return 35

    score = 0

    for k in kws:
        kk = norm(k)
        if not kk or kk == c_norm:
            continue

        is_phrase = " " in kk
        is_generic = is_generic_kw(kk)

        hit = (kk in t) if is_phrase else (kk in tset)
        if not hit:
            continue

        if is_generic:
            score += 0
        elif is_phrase:
            score += 6
        else:
            score += 3

    return min(score, 35)


# -------------------------------------------------
# Estudio status
# -------------------------------------------------

def infer_study_status(text: str) -> str:
    t = norm(text)

    estudia = [
        "studying", "estudiante", "currently studying",
        "en curso", "cursando", "student at"
    ]

    estudio = [
        "graduated", "alumni", "egresad",
        "class of", "licenciad",
        "bsc", "msc", "mba", "graduation"
    ]

    if any(k in t for k in estudia):
        return "ESTUDIA"

    if any(k in t for k in estudio):
        return "ESTUDIÓ"

    return "NO DETERMINADO"


# -------------------------------------------------
# Name closeness — EC
# -------------------------------------------------

def name_closeness_for_item(estudiante: str, item: dict) -> int:
    """
    Métrica comparativa para desempatar candidatos válidos.
    NO es un gate, solo sirve para elegir el mejor del TOP.
    """
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    slug = extract_slug(url)
    slug_tokens = token_set(slug)
    txt_tokens = token_set(f"{title} {snippet}")

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)

    score = 0

    # Apellidos pesan fuerte (soporta apellidos compuestos)
    if _ap_in_slug(ap1, slug, slug_tokens):
        score += 60
    if _ap_in_slug(ap2, slug, slug_tokens):
        score += 45

    # Nombres
    for n in nombres:
        if n in slug_tokens or n in txt_tokens:
            score += 18 if n not in COMMON_GIVEN else 6
        else:
            score -= 8

    # Pequeño ajuste fuzzy
    score += int(fuzzy_ratio(estudiante, f"{slug} {title}") * 0.1)

    return score


# -------------------------------------------------
# MAIN SCORING — EC
# -------------------------------------------------

def score_candidate(carrera: str, estudiante: str, item: dict,
                    anio_graduacion=None, universidad: str = ""):

    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    if not is_profile_url(url):
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {}

    slug = extract_slug(url)
    text = f"{title} {snippet} {slug}"
    t = norm(text)

    # SLUG GATING
    if not slug_gating_pass(estudiante, url, f"{title} {snippet}"):
        return 0, {"udla": False, "carrera": False, "nombre": False, "anio": None}, {}

    # UDLA OBLIGATORIO
    if not udla_ec_required(t, universidad):
        return 0, {"udla": False, "carrera": False, "nombre": True, "anio": None}, {}

    # SCORING
    nombres, ap1, ap2 = split_nombre_dataset(estudiante)
    slug_tokens = token_set(slug)
    txt_tokens = token_set(t)

    ap1_in_slug = _ap_in_slug(ap1, slug, slug_tokens)
    ap2_in_slug = _ap_in_slug(ap2, slug, slug_tokens)

    score = 60

    cs = career_score(t, carrera)
    score += cs

    if ap1_in_slug:
        score += 30
    if ap2_in_slug:
        score += 20

    for n in nombres:
        if n in slug_tokens or n in txt_tokens:
            score += 10

    sim2 = fuzzy_ratio(estudiante, f"{slug} {title}")
    if sim2 >= 92:
        score += 10

    # AÑO DE GRADUACIÓN
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
