from __future__ import annotations

from utils.text_norm import norm, simple_tokens, token_set, contains_any_smart
from utils.carreras_synonyms import expand_carrera_keywords, is_generic_kw


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
        u = (url or "")
        slug = u.split("/in/")[1]
        slug = slug.split("/")[0]
        slug = slug.replace("-", " ")
        return norm(slug)
    except Exception:
        return ""


def country_hint_from_url(url: str) -> str:
    u = (url or "").lower()
    if "://ec.linkedin.com" in u:
        return "ec"
    return ""


# -------------------------------------------------
# DATASET PARSING
# Formato: APELLIDO1 APELLIDO2 NOMBRE1 NOMBRE2
# -------------------------------------------------

CONNECTORS = {"de", "del", "la", "las", "los", "y", "san", "santa", "da", "do", "dos", "von", "van"}

COMMON_GIVEN = {
    "ana", "maria", "jose", "juan", "luis", "carlos",
    "pedro", "javier", "paola", "paula", "andres",
    "fernando", "daniel", "david", "ivan", "alex",
    "angel", "miguel", "sofia", "camila"
}


def split_nombre_dataset(nombre: str):
    parts = [p for p in norm(nombre).split() if len(p) >= 2]
    if len(parts) < 3:
        return [], None, None

    ap1 = parts[0]
    ap2 = parts[1]
    nombres = parts[2:]

    return nombres, ap1, ap2


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
# SLUG GATING ESTRICTO
# -------------------------------------------------

def slug_gating_pass(estudiante: str, url: str, title_snippet_text: str):

    slug = extract_slug(url)
    slug_tokens = token_set(slug)
    txt_tokens = token_set(title_snippet_text)

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)

    # Apellido obligatorio en slug
    ap_ok = False
    if ap1 and ap1 in slug_tokens:
        ap_ok = True
    if ap2 and ap2 in slug_tokens:
        ap_ok = True

    if not ap_ok:
        return False, {"reason": "no_apellido_slug", "slug": slug}

    # Al menos un nombre obligatorio
    nombre_ok = False
    for n in nombres:
        if n and (n in slug_tokens or n in txt_tokens):
            nombre_ok = True
            break

    if not nombre_ok:
        return False, {"reason": "no_nombre", "slug": slug}

    sim = fuzzy_ratio(estudiante, f"{slug} {title_snippet_text}")

    return True, {"slug": slug, "sim": sim}


# -------------------------------------------------
# NAME CLOSENESS (para elegir mejor del TOP)
# -------------------------------------------------

def name_closeness_for_item(estudiante: str, item: dict) -> int:
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    slug = extract_slug(url)
    st = token_set(slug)
    tt = token_set(f"{title} {snippet}")

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)
    expected = set([x for x in [ap1, ap2] if x] + nombres)

    score = 0

    if ap1 and ap1 in st:
        score += 60
    if ap2 and ap2 in st:
        score += 45

    for n in nombres:
        if n in st or n in tt:
            score += 18 if n not in COMMON_GIVEN else 6
        else:
            score -= 10

    extras = [x for x in st if x.isalpha() and len(x) >= 3 and x not in expected]
    score -= min(25, len(extras) * 3)

    score += int(fuzzy_ratio(estudiante, f"{slug} {title}") * 0.1)

    return score


# -------------------------------------------------
# Career score refinado
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
# Estudio status (AGREGADO para evitar ImportError)
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
# MAIN SCORING
# -------------------------------------------------

def score_candidate(carrera: str, estudiante: str, item: dict, expected_country: str = "ec"):

    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    if not is_profile_url(url):
        return 0, {"udla": False, "carrera": False, "nombre": False}, {"penal": -999}

    slug = extract_slug(url)
    text = f"{title} {snippet} {slug}"
    t = norm(text)

    reasons = {"udla": 0, "carrera": 0, "nombre": 0, "pais": 0}

    # SLUG GATING
    passed, _ = slug_gating_pass(estudiante, url, f"{title} {snippet}")
    if not passed:
        return 0, {"udla": False, "carrera": False, "nombre": False}, {"penal": -999}

    # Ecuador obligatorio
    if expected_country == "ec":
        ecuador_terms = ["ecuador", "quito", "guayaquil", "cuenca"]
        url_country = country_hint_from_url(url)

        if url_country != "ec" and not contains_any_smart(t, ecuador_terms):
            return 0, {"udla": False, "carrera": False, "nombre": True}, {"penal": -999}

    # UDLA obligatorio
    udla_terms = ["udla", "universidad de las americas", "universidad de las américas"]

    if not contains_any_smart(t, udla_terms):
        return 0, {"udla": False, "carrera": False, "nombre": True}, {"penal": -999}

    # SCORING
    score = 50  # UDLA base
    reasons["udla"] += 50

    cs = career_score(t, carrera)
    score += cs
    reasons["carrera"] += cs

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)

    slug_tokens = token_set(slug)
    txt_tokens = token_set(t)

    name_score = 0

    if ap1 and ap1 in slug_tokens:
        name_score += 40
    if ap2 and ap2 in slug_tokens:
        name_score += 30

    for n in nombres:
        if n in slug_tokens or n in txt_tokens:
            name_score += 15

    score += name_score
    reasons["nombre"] += name_score

    sim2 = fuzzy_ratio(estudiante, f"{slug} {title}")
    if sim2 >= 92:
        score += 10

    score = max(0, min(100, score))

    flags = {
        "udla": True,
        "carrera": reasons["carrera"] > 0,
        "nombre": reasons["nombre"] > 0
    }

    return score, flags, reasons