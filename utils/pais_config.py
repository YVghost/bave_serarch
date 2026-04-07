from __future__ import annotations

from utils.text_norm import norm, simple_tokens, token_set
from utils.carreras_synonyms import expand_carrera_keywords

# Palabras vacías que no aportan al matching de universidad
_UNIV_STOPWORDS = {"de", "del", "la", "las", "los", "el", "y", "e", "a", "en"}


# -------------------------------------------------
# Tags de universidad por país (fallback cuando no
# hay campo "Universidad" en el Excel)
# -------------------------------------------------

UDLA_TAGS: dict[str, str] = {
    "EC": '("UDLA (EC)" OR "Universidad de Las Américas (EC)" OR "Universidad de las Américas (EC)")',
    "CR": '("UDLA (CR)" OR "Universidad de Las Américas (CR)" OR "Universidad de las Américas (CR)")',
}


# -------------------------------------------------
# Fragmento de universidad para la query de búsqueda
# -------------------------------------------------

_CR_UNIV_GENERIC = {"universidad", "college", "instituto", "escuela", "de", "del", "la", "las", "los", "el", "y", "en", "a"}


def _short_cr_university(universidad: str) -> str:
    """
    Genera la forma corta de una universidad CR para usar en OR dentro de la query.
    "Universidad Latina de Costa Rica" → "Universidad Latina"
    "Instituto Tecnológico de CR"      → "Instituto Tecnológico"
    """
    tokens = simple_tokens(universidad)
    # Tomar el primer token genérico (universidad/instituto…) + el primer token distintivo
    result = []
    for t in tokens:
        result.append(t)
        if t not in _CR_UNIV_GENERIC and len(result) >= 2:
            break
    short = " ".join(result)
    full  = norm(universidad)
    # Solo tiene sentido si es más corta que el nombre completo
    return short if short and short != full else ""


def university_query_part(pais: str, universidad: str = "") -> str:
    """
    CR con universidad específica:
        Usa nombre completo OR forma corta para maximizar cobertura.
        Ej: ("Universidad Latina de Costa Rica" OR "Universidad Latina")
    EC o sin universidad:
        Usa el tag hardcodeado de UDLA_TAGS.
    """
    if pais == "CR" and universidad:
        full  = universidad.strip()
        short = _short_cr_university(full)
        if short and norm(short) != norm(full):
            return f'("{full}" OR "{short}")'
        return f'"{full}"'
    return UDLA_TAGS[pais]


# -------------------------------------------------
# Constructor de query
# -------------------------------------------------

def build_query(
    nombre_var: str,
    carrera: str,
    mode: str,
    pais: str = "EC",
    universidad: str = "",
    max_terms: int = 4,
) -> str:
    """
    Construye la query de búsqueda de LinkedIn para Serper.

    Modos:
      name_udlaec  → nombre + universidad (sin carrera, query rápida)
      strict       → nombre + carrera + universidad (query detallada)
    """
    base = f'site:linkedin.com/in ("{nombre_var}")'
    univ_part = university_query_part(pais, universidad)

    if mode == "name_udlaec":
        return f"{base} {univ_part}"

    kws = expand_carrera_keywords(carrera) or []
    picked: list[str] = []
    seen: set[str] = set()

    for k in kws:
        k = (k or "").strip()
        if not k or k.lower() in seen:
            continue
        picked.append(k)
        seen.add(k.lower())
        if len(picked) >= max_terms:
            break

    carrera_part = (
        "(" + " OR ".join([f'"{k}"' for k in picked]) + ")"
        if picked
        else f'"{carrera}"'
    )

    return f"{base} {carrera_part} {univ_part}"


# -------------------------------------------------
# Validación de universidad (gate de scoring)
# -------------------------------------------------

def _significant_tokens(name: str) -> list[str]:
    """Tokens del nombre de universidad excluyendo palabras vacías."""
    return [t for t in simple_tokens(name) if t not in _UNIV_STOPWORDS and len(t) > 1]


def validate_universidad(t: str, pais: str = "EC", universidad: str = "") -> bool:
    """
    Valida que el texto normalizado de un perfil de LinkedIn mencione
    la universidad esperada. Recibe 't' ya normalizado (resultado de norm()).

    CR con universidad específica — dos niveles:
      1. Substring exacto del nombre normalizado en el texto.
      2. Fallback: todos los tokens significativos del nombre aparecen
         en el texto (maneja abreviaciones y variaciones de formato).
         Ej: "Universidad Latina de Costa Rica" → tokens significativos
             ["universidad", "latina", "costa", "rica"] deben estar todos.

    EC (o CR sin universidad):
        El texto debe contener el tag de país (ec)/(cr) Y mencionar
        UDLA o Universidad de las Américas.
    """
    if pais == "CR" and universidad:
        u_norm = norm(universidad)

        # Nivel 1: substring exacto
        if u_norm in t:
            return True

        # Nivel 2: todos los tokens significativos presentes
        sig = _significant_tokens(universidad)
        if sig:
            t_tokens = token_set(t)
            return all(tok in t_tokens for tok in sig)

        return False

    tag = f"({pais.lower()})"
    if tag not in t:
        return False

    if "udla" in t:
        return True

    if "universidad de las americas" in t:
        return True

    return False
