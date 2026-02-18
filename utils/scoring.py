# utils/scoring.py
# Scoring completo (UDLA + carrera + nombre + ubicación + penalizaciones) + inferencia estudia/estudió
# Nota: este archivo asume que en utils/text_norm.py existen norm(), simple_tokens() o equivalentes.
# Si tu text_norm solo tiene norm(), abajo incluyo implementaciones de contains_any y count_hits aquí mismo.

from __future__ import annotations

from utils.text_norm import norm
from utils.carreras_synonyms import expand_carrera_keywords  # usa el nombre que te di en carreras_synonyms.py

# -------- Helpers locales (para no depender de funciones faltantes en text_norm) --------

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
    return ("linkedin.com/in/" in u) and not any(x in u for x in ["/jobs", "/company", "/school", "/posts", "/pulse"])

def infer_study_status(text: str) -> str:
    t = norm(text)

    # señales de "estudia"
    estudia = [
        "studying", "estudiante", "currently studying", "en curso", "cursando",
        "student at", "undergraduate", "postgraduate student", "master student"
    ]

    # señales de "estudió"
    estudio = [
        "graduated", "alumni", "egresad", "ex-", "class of",
        "licenciad", "bsc", "msc", "mba", "ing.", "graduation"
    ]

    if any(k in t for k in estudia):
        return "ESTUDIA"
    if any(k in t for k in estudio):
        return "ESTUDIÓ"
    return "NO DETERMINADO"

def score_candidate(carrera: str, estudiante: str, item: dict) -> tuple[int, dict, dict]:
    """
    item esperado:
      {"url": "...", "title": "...", "snippet": "..."}  (lo devuelve el buscador)

    Retorna:
      score (int),
      flags: {"udla": bool, "carrera": bool, "nombre": bool},
      reasons: {"udla": int, "carrera": int, "nombre": int, "ecuador": int, "penal": int}
    """
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""
    text = f"{title} {snippet} {url}"
    t = norm(text)

    score = 0
    reasons = {"udla": 0, "carrera": 0, "nombre": 0, "ecuador": 0, "penal": 0}

    # ---------------- UDLA ----------------
    udla_terms = ["udla", "universidad de las americas", "universidad de las américas"]
    udla_hit = contains_any(t, udla_terms)

    # puntaje por señales UDLA (acumulativo)
    if "udla" in t:
        score += 40; reasons["udla"] += 40
    if "universidad de las americas" in t:
        score += 40; reasons["udla"] += 40

    # ---------------- Carrera (multi-idioma + familias + manual) ----------------
    # IMPORTANTE: en mi carreras_synonyms.py la función se llama expand_carrera_keywords
    career_kws = expand_carrera_keywords(carrera)
    hits = count_hits(t, career_kws)

    carrera_hit = False

    # match fuerte: carrera completa exacta normalizada
    if career_kws and career_kws[0] and norm(career_kws[0]) in t:
        score += 35; reasons["carrera"] += 35
        carrera_hit = True
    else:
        # match por keywords
        if hits >= 3:
            score += 25; reasons["carrera"] += 25
            carrera_hit = True
        elif hits == 2:
            score += 20; reasons["carrera"] += 20
            carrera_hit = True
        elif hits == 1:
            score += 10; reasons["carrera"] += 10
            carrera_hit = True

    # ---------------- Nombre (suave) ----------------
    est = norm(estudiante)
    parts = [p for p in est.split() if len(p) >= 3]

    nombre_hit = False
    if len(parts) >= 2:
        # intenta que al menos 2 partes del nombre estén en título/snippet
        name_hits = sum(1 for p in parts[:4] if p in t)
        if name_hits >= 2:
            score += 20; reasons["nombre"] += 20
            nombre_hit = True
        elif name_hits == 1:
            score += 8; reasons["nombre"] += 8

    # ---------------- Ecuador (opcional) ----------------
    if any(k in t for k in ["ecuador", "quito", "guayaquil", "cuenca", "ambato", "manta"]):
        score += 8; reasons["ecuador"] += 8

    # ---------------- Penalizaciones ----------------
    # otras universidades frecuentes (ajusta a tu caso)
    other_uni_terms = ["usfq", "puce", "puc", "uce", "espol", "uide", "utpl", "ucuenca", "uceva"]
    if contains_any(t, other_uni_terms):
        score -= 15; reasons["penal"] -= 15

    # si no parece perfil real, penaliza fuerte (pero no descartes si igual quieres revisar)
    if not is_profile_url(url):
        score -= 40; reasons["penal"] -= 40

    flags = {"udla": udla_hit, "carrera": carrera_hit, "nombre": nombre_hit}
    return score, flags, reasons
