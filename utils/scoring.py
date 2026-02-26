from __future__ import annotations

from utils.text_norm import norm, simple_tokens, token_set, contains_any_smart
from utils.carreras_synonyms import expand_carrera_keywords, is_generic_kw


# -------------------------
# LinkedIn helpers
# -------------------------

def is_profile_url(url: str) -> bool:
    u = (url or "").lower()
    return ("linkedin.com/in/" in u) and not any(
        x in u for x in ["/jobs", "/company", "/school", "/posts", "/pulse"]
    )


def extract_slug(url: str) -> str:
    """
    https://.../in/paola-garces-salazarar-b753263b -> "paola garces salazarar b753263b"
    """
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
    if "://ec.linkedin.com" in u or "//ec.linkedin.com" in u:
        return "ec"
    if "://cl.linkedin.com" in u or "//cl.linkedin.com" in u:
        return "cl"
    if "://co.linkedin.com" in u or "//co.linkedin.com" in u:
        return "co"
    if "://pe.linkedin.com" in u or "//pe.linkedin.com" in u:
        return "pe"
    if "://mx.linkedin.com" in u or "//mx.linkedin.com" in u:
        return "mx"
    if "://hn.linkedin.com" in u or "//hn.linkedin.com" in u:
        return "hn"
    if "://ve.linkedin.com" in u or "//ve.linkedin.com" in u:
        return "ve"
    if "://ca.linkedin.com" in u or "//ca.linkedin.com" in u:
        return "ca"
    if "://de.linkedin.com" in u or "//de.linkedin.com" in u:
        return "de"
    return ""


# -------------------------
# Nombre parsing (dataset: APELLIDO1 APELLIDO2 NOMBRE1 NOMBRE2...)
# -------------------------

CONNECTORS = {"de", "del", "la", "las", "los", "y", "san", "santa", "da", "do", "dos", "von", "van"}

# nombres MUY comunes (aportan poco para distinguir)
COMMON_GIVEN = {
    "ana", "maria", "jose", "juan", "luis", "carlos", "pedro", "javier",
    "paola", "paula", "andres", "fernando", "daniel", "david", "ivan",
    "cristian", "alex", "angel", "miguel", "sofia", "camila", "gabriela",
    "carolina", "valentina", "mariana", "victoria"
}

def split_nombre_dataset(nombre: str):
    parts = [p for p in norm(nombre).split() if len(p) >= 2]
    if len(parts) < 3:
        return [], None, None

    ap1 = parts[0]
    ap2 = parts[1] if len(parts) >= 2 else None
    nombres = parts[2:] if len(parts) >= 3 else []
    return nombres, ap1, ap2


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
        inter = len(sa & sb)
        union = len(sa | sb)
        return int(round((inter / union) * 100))


def name_token_score(text: str, estudiante: str) -> tuple[int, dict]:
    tset = token_set(text)
    nombres, ap1, ap2 = split_nombre_dataset(estudiante)

    ap_hits = 0
    nm_hits = 0

    if ap1 and ap1 in tset:
        ap_hits += 1
    if ap2 and ap2 in tset:
        ap_hits += 1

    for n in nombres:
        if n and n in tset:
            nm_hits += 1

    score = 0
    if ap_hits >= 1:
        score += 30
    if ap_hits == 2:
        score += 10
    if nm_hits >= 1:
        score += 15
    if nm_hits >= 2:
        score += 10

    details = {"ap_hits": ap_hits, "nm_hits": nm_hits, "ap1": ap1, "ap2": ap2, "nombres": nombres}
    return min(score, 60), details


def slug_gating_pass(estudiante: str, url: str, title_snippet_text: str) -> tuple[bool, dict]:
    """
    Anti-falsos-positivos:
    - slug contiene al menos 1 apellido (ap1 o ap2)
    - y al menos 1 nombre (no conector) en slug o title/snippet
    - o fuzzy >= 88
    """
    slug = extract_slug(url)
    slug_tokens = token_set(slug)
    txt_tokens = token_set(title_snippet_text)

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)

    ap_ok = False
    if ap1 and ap1 in slug_tokens:
        ap_ok = True
    if ap2 and ap2 in slug_tokens:
        ap_ok = True

    non_connector_names = [n for n in nombres if n not in CONNECTORS]

    nm_ok = False
    for n in non_connector_names:
        if n in slug_tokens or n in txt_tokens:
            nm_ok = True
            break

    sim = fuzzy_ratio(estudiante, f"{slug} {title_snippet_text}")

    passed = (ap_ok and nm_ok) or (sim >= 88)
    meta = {"slug": slug, "ap_ok": ap_ok, "nm_ok": nm_ok, "sim": sim}
    return passed, meta


# -------------------------
# NUEVO: Name closeness (para elegir el "best_url real")
# -------------------------

def name_closeness_for_item(estudiante: str, item: dict) -> int:
    """
    Metrica para escoger el link "real" dentro del TOP:
    - recompensa apellidos y nombres distintivos presentes en SLUG
    - penaliza nombres extra en slug que NO están en el estudiante
    """
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    slug = extract_slug(url)
    st = token_set(slug)  # tokens del slug
    tt = token_set(f"{title} {snippet}")  # tokens del title/snippet

    nombres, ap1, ap2 = split_nombre_dataset(estudiante)
    expected = set([x for x in [ap1, ap2] if x] + [n for n in nombres if n and n not in CONNECTORS])

    score = 0

    # apellidos pesan fuerte
    if ap1 and ap1 in st:
        score += 60
    if ap2 and ap2 in st:
        score += 45

    # nombres (distintivos pesan más)
    for n in [x for x in nombres if x and x not in CONNECTORS]:
        if n in st or n in tt:
            score += 18 if n not in COMMON_GIVEN else 6
        else:
            # si falta un nombre distintivo, castiga
            score -= 12 if n not in COMMON_GIVEN else 0

    # penaliza "ruido": tokens alfabéticos del slug que no pertenecen al estudiante
    extras = [x for x in st if x.isalpha() and len(x) >= 3 and x not in expected]
    score -= min(30, 3 * len(extras))

    # fuzzy pequeño para casos raros
    score += int(round(fuzzy_ratio(estudiante, f"{slug} {title}") * 0.10))  # 0..10 aprox

    return score


# -------------------------
# Carrera scoring (ponderado)
# -------------------------

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
            score += 1
        elif is_phrase:
            score += 8
        else:
            score += 4

    return min(score, 35)


# -------------------------
# Estudio status
# -------------------------

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


# -------------------------
# Scoring principal con ECUADOR + UDLA obligatorios
# -------------------------

def score_candidate(carrera: str, estudiante: str, item: dict, expected_country: str = "ec"):
    url = item.get("url", "") or ""
    title = item.get("title", "") or ""
    snippet = item.get("snippet", "") or ""

    if not is_profile_url(url):
        return 0, {"udla": False, "carrera": False, "nombre": False}, {
            "udla": 0, "carrera": 0, "nombre": 0, "pais": 0, "penal": -999
        }

    slug = extract_slug(url)
    text = f"{title} {snippet} {slug}"
    t = norm(text)

    reasons = {"udla": 0, "carrera": 0, "nombre": 0, "pais": 0, "penal": 0}

    # gate nombre/slug
    passed, meta = slug_gating_pass(estudiante, url, f"{title} {snippet}")
    if not passed:
        return 0, {"udla": False, "carrera": False, "nombre": False}, {
            **reasons, "penal": -999, "debug_slug": meta.get("slug", ""), "debug_sim": meta.get("sim", 0)
        }

    # gate Ecuador obligatorio
    ecuador_terms = ["ecuador", "quito", "guayaquil", "cuenca", "ambato", "manta"]
    url_country = country_hint_from_url(url)

    ecuador_ok = False
    if expected_country == "ec":
        if url_country == "ec":
            ecuador_ok = True
        elif contains_any_smart(t, ecuador_terms):
            ecuador_ok = True

        if not ecuador_ok:
            return 0, {"udla": False, "carrera": False, "nombre": True}, {
                **reasons, "penal": -999, "debug_slug": meta.get("slug", ""), "debug_sim": meta.get("sim", 0)
            }

    # gate UDLA obligatorio
    udla_terms = ["udla", "universidad de las americas", "universidad de las américas"]
    udla_ok = contains_any_smart(t, udla_terms)

    if not udla_ok:
        return 0, {"udla": False, "carrera": False, "nombre": True}, {
            **reasons, "penal": -999, "debug_slug": meta.get("slug", ""), "debug_sim": meta.get("sim", 0)
        }

    # score
    score = 0

    score += 50
    reasons["udla"] += 50

    if url_country == "ec":
        score += 5
        reasons["pais"] += 5

    cs = career_score(t, carrera)
    score += cs
    reasons["carrera"] += cs

    name_sc, name_meta = name_token_score(t, estudiante)
    score += name_sc
    reasons["nombre"] += name_sc

    sim2 = fuzzy_ratio(estudiante, f"{slug} {title}")
    if sim2 >= 92:
        score += 10
        reasons["nombre"] += 10
    elif sim2 >= 85:
        score += 6
        reasons["nombre"] += 6

    score = max(0, min(100, score))

    flags = {"udla": True, "carrera": reasons["carrera"] > 0, "nombre": reasons["nombre"] > 0}

    return score, flags, {
        **reasons,
        "debug_slug": meta.get("slug", ""),
        "debug_sim": meta.get("sim", 0),
        "debug_name": name_meta,
        "debug_sim2": sim2,
    }