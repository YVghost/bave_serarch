from __future__ import annotations

from utils.text_norm import norm
from utils.carreras_synonyms import expand_carrera_keywords


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def contains_any(haystack: str, needles: list[str]) -> bool:
    h = norm(haystack)
    for n in needles:
        nn = norm(n)
        if nn and nn in h:
            return True
    return False


def count_hits(haystack: str, needles: list[str]) -> int:
    h = norm(haystack)
    hits = 0
    for n in needles:
        nn = norm(n)
        if nn and nn in h:
            hits += 1
    return hits


def is_profile_url(url: str) -> bool:
    u = (url or "").lower()
    return ("linkedin.com/in/" in u) and not any(
        x in u for x in ["/jobs", "/company", "/school", "/posts", "/pulse"]
    )


def split_nombre(nombre: str):
    """
    Divide nombre completo en:
    - nombres
    - apellido1
    - apellido2 (si existe)
    """
    parts = [p for p in norm(nombre).split() if len(p) >= 3]

    if len(parts) >= 3:
        return parts[:-2], parts[-2], parts[-1]
    elif len(parts) == 2:
        return [parts[0]], parts[1], None
    elif len(parts) == 1:
        return [parts[0]], None, None
    else:
        return [], None, None


# -------------------------------------------------
# Inferencia estudio / estudió
# -------------------------------------------------

def infer_study_status(text: str) -> str:
    t = norm(text)

    estudia = [
        "studying", "estudiante", "currently studying",
        "en curso", "cursando", "student at"
    ]

    estudio = [
        "graduated", "alumni", "egresad",
        "class of", "licenciad", "bsc",
        "msc", "mba", "graduation"
    ]

    if any(k in t for k in estudia):
        return "ESTUDIA"
    if any(k in t for k in estudio):
        return "ESTUDIÓ"
    return "NO DETERMINADO"


# -------------------------------------------------
# SCORING PRINCIPAL
# -------------------------------------------------

def score_candidate(carrera: str, estudiante: str, item: dict):

    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    text = f"{title} {snippet} {url}"
    t = norm(text)

    score = 0
    reasons = {
        "udla": 0,
        "carrera": 0,
        "nombre": 0,
        "ecuador": 0,
        "penal": 0
    }

    # -------------------------------------------------
    # 1️⃣ Debe ser perfil LinkedIn real
    # -------------------------------------------------

    if not is_profile_url(url):
        return 0, {"udla": False, "carrera": False, "nombre": False}, {
            **reasons,
            "penal": -999
        }

    # -------------------------------------------------
    # 2️⃣ UDLA
    # -------------------------------------------------

    udla_terms = [
        "udla",
        "universidad de las americas",
        "universidad de las américas"
    ]

    if contains_any(t, udla_terms):
        score += 50
        reasons["udla"] += 50

    # -------------------------------------------------
    # 3️⃣ Carrera
    # -------------------------------------------------

    career_kws = expand_carrera_keywords(carrera)
    hits = count_hits(t, career_kws)

    if career_kws and norm(career_kws[0]) in t:
        score += 35
        reasons["carrera"] += 35
    elif hits >= 3:
        score += 25
        reasons["carrera"] += 25
    elif hits == 2:
        score += 20
        reasons["carrera"] += 20
    elif hits == 1:
        score += 10
        reasons["carrera"] += 10

    # -------------------------------------------------
    # 4️⃣ Nombre estructural (OBLIGATORIO ≥1 apellido)
    # -------------------------------------------------

    nombres, apellido1, apellido2 = split_nombre(estudiante)

    apellido_match = False

    if apellido1 and apellido1 in t:
        score += 35
        reasons["nombre"] += 35
        apellido_match = True

    if apellido2 and apellido2 in t:
        score += 30
        reasons["nombre"] += 30
        apellido_match = True

    # ❌ DESCARTE AUTOMÁTICO SI NO COINCIDE NINGÚN APELLIDO
    if not apellido_match:
        return 0, {"udla": False, "carrera": False, "nombre": False}, {
            **reasons,
            "penal": -999
        }

    # Coincidencia de nombres (peso menor)
    nombre_hits = sum(1 for n in nombres if n in t)

    if nombre_hits >= 2:
        score += 15
        reasons["nombre"] += 15
    elif nombre_hits == 1:
        score += 8
        reasons["nombre"] += 8

    # -------------------------------------------------
    # 5️⃣ Ecuador
    # -------------------------------------------------

    ecuador_terms = [
        "ecuador", "quito", "guayaquil",
        "cuenca", "ambato", "manta"
    ]

    if contains_any(t, ecuador_terms):
        score += 10
        reasons["ecuador"] += 10
    else:
        score -= 20
        reasons["penal"] -= 20

    # -------------------------------------------------
    # 6️⃣ Penalización otras universidades
    # -------------------------------------------------

    other_uni_terms = [
        "usfq", "puce", "puc",
        "uce", "espol", "uide",
        "utpl", "ucuenca"
    ]

    if contains_any(t, other_uni_terms):
        score -= 20
        reasons["penal"] -= 20

    # -------------------------------------------------
    # Normalización final
    # -------------------------------------------------

    if score < 0:
        score = 0
    if score > 100:
        score = 100

    flags = {
        "udla": reasons["udla"] > 0,
        "carrera": reasons["carrera"] > 0,
        "nombre": reasons["nombre"] > 0
    }

    return score, flags, reasons