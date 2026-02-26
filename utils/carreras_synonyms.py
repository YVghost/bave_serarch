from __future__ import annotations

from utils.text_norm import norm, simple_tokens, ACRONYMS_OK

_STOP = {
    "de", "del", "la", "el", "en", "y", "e", "para", "con", "a",
    "the", "and", "of", "in", "to", "for", "mention", "mencion", "mención"
}

# Palabras demasiado genéricas: NO deberían sumar fuerte
_GENERIC = {
    "business", "management", "engineering", "developer", "programming",
    "legal", "health", "science", "data", "analytics"
}


def _clean_tokens(s: str) -> list[str]:
    """
    Mantiene tokens >=3, PERO permite siglas útiles de 2 letras.
    """
    out = []
    for t in simple_tokens(s):
        if t in _STOP:
            continue
        if len(t) >= 3 or t in ACRONYMS_OK:
            out.append(t)
    return out


def _dedup_preserve(seq: list[str]) -> list[str]:
    out, seen = [], set()
    for x in seq:
        x = norm(x)
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


# --- MAPEOS MANUALES EXACTOS (puedes ampliar con el tuyo completo) ---
_MANUAL_EXACT = {
    norm("ADMINISTRACION DE EMPRESAS"): ["business administration", "business", "management", "mba"],
    norm("NEGOCIOS INTERNACIONALES"): ["international business", "global business", "trade", "comercio internacional"],
    norm("INGENIERIA DE SOFTWARE"): ["software engineering", "software engineer", "computer science", "cs", "it", "developer"],
    norm("PUBLICIDAD"): ["advertising", "ads", "marketing", "branding"],
    norm("DISEÑO DE INTERIORES"): ["interior design", "interior designer"],
    norm("DERECHO"): ["law", "legal", "jurisprudence"],
}

# --- FAMILIAS (expansión débil) ---
_FAMILY_EQUIV = {
    "software": ["software engineering", "computer science", "it", "developer", "programming", "informatics"],
    "administracion": ["business administration", "management", "business"],
    "negocios": ["business", "international business", "trade"],
    "publicidad": ["advertising", "marketing", "branding"],
    "diseno": ["design", "graphic design", "interior design", "product design"],
    "derecho": ["law", "legal"],
}


def expand_carrera_keywords(carrera: str) -> list[str]:
    """
    Keywords para matching en snippets/títulos.
    Incluye:
      - carrera completa
      - equivalencias manuales
      - tokens limpios
      - expansión por familias detectadas
    """
    c_norm = norm(carrera)
    out: list[str] = []

    if c_norm:
        out.append(c_norm)

    if c_norm in _MANUAL_EXACT:
        out.extend(_MANUAL_EXACT[c_norm])

    ts = _clean_tokens(carrera)
    out.extend(ts)

    for t in ts:
        if t in _FAMILY_EQUIV:
            out.extend(_FAMILY_EQUIV[t])

    if "ingenieria" in ts:
        out.append("engineering")

    return _dedup_preserve(out)


def is_generic_kw(kw: str) -> bool:
    return norm(kw) in _GENERIC