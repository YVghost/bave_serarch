from __future__ import annotations

from utils.text_norm import norm, simple_tokens, token_set
from utils.carreras_synonyms import expand_carrera_keywords

_UNIV_STOPWORDS = {"de", "del", "la", "las", "los", "el", "y", "e", "a", "en"}

UDLA_TAGS: dict[str, str] = {
    "EC": '("UDLA (EC)" OR "Universidad de Las Américas (EC)" OR "Universidad de las Américas (EC)")',
    "CR": '("UDLA (CR)" OR "Universidad de Las Américas (CR)" OR "Universidad de las Américas (CR)")',
}

_CR_UNIV_GENERIC = {
    "universidad", "college", "instituto", "escuela",
    "de", "del", "la", "las", "los", "el", "y", "en", "a"
}


def _short_cr_university(universidad: str) -> str:
    """
    "Universidad Latina de Costa Rica" -> "Universidad Latina"
    """
    tokens = simple_tokens(universidad)
    result = []
    for t in tokens:
        result.append(t)
        if t not in _CR_UNIV_GENERIC and len(result) >= 2:
            break
    short = " ".join(result)
    full = norm(universidad)
    return short if short and short != full else ""


def university_query_part(pais: str, universidad: str = "") -> str:
    if pais == "CR" and universidad:
        full = universidad.strip()
        short = _short_cr_university(full)
        if short and norm(short) != norm(full):
            return f'("{full}" OR "{short}" OR "ULatina" OR "U Latina")'
        return f'("{full}" OR "ULatina" OR "U Latina")'
    return UDLA_TAGS[pais]


def build_query(
    nombre_var: str,
    carrera: str,
    mode: str,
    pais: str = "EC",
    universidad: str = "",
    max_terms: int = 4,
) -> str:
    base = f'site:linkedin.com/in ("{nombre_var}")'
    univ_part = university_query_part(pais, universidad)

    if mode == "name_udlaec":
        return f"{base} {univ_part}"

    if mode == "name_country" and pais == "CR":
        return f'{base} ("Costa Rica" OR "CR" OR "ULatina" OR "Universidad Latina")'

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


def _significant_tokens(name: str) -> list[str]:
    return [t for t in simple_tokens(name) if t not in _UNIV_STOPWORDS and len(t) > 1]


def validate_universidad(t: str, pais: str = "EC", universidad: str = "", strict: bool = False) -> bool:
    """
    CR:
      strict=True  -> nombre exacto
      strict=False -> nombre exacto O todos los tokens significativos presentes
    """
    if pais == "CR" and universidad:
        u_norm = norm(universidad)

        if u_norm in t:
            return True

        # alias frecuentes
        if "universidad latina" in t or "ulatina" in t or "u latina" in t:
            if "costa rica" in t or "cr" in t:
                return True
            # incluso si no aparece CR, lo dejamos pasar en modo no estricto
            if not strict:
                return True

        if not strict:
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