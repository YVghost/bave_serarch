from __future__ import annotations

import re
from unidecode import unidecode

# Siglas de 2 letras útiles que NO debemos botar
ACRONYMS_OK = {
    "it", "cs", "bi", "ds", "ir", "md", "pt", "mba", "pmp",
    "hse", "osh", "sso", "scm", "omt", "dvm", "vet"
}


def norm(s: str) -> str:
    """
    Normaliza:
    - convierte a str
    - quita tildes
    - lower
    - limpia espacios raros (incluye NBSP y zero-width)
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\u00A0", " ")   # NBSP
    s = s.replace("\u200B", "")   # zero-width
    s = unidecode(s).lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def simple_tokens(s: str) -> list[str]:
    """
    Tokens (letras/números) ya normalizados.
    """
    s = norm(s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return [t for t in s.split() if t]


def token_set(s: str) -> set[str]:
    return set(simple_tokens(s))


def contains_any_smart(text: str, needles: list[str]) -> bool:
    """
    - Frases (con espacios): substring sobre text normalizado
    - Tokens (una palabra/sigla): match exacto por token_set
    """
    t = norm(text)
    ts = token_set(t)
    for n in needles:
        nn = norm(n)
        if not nn:
            continue
        if " " in nn:
            if nn in t:
                return True
        else:
            if nn in ts:
                return True
    return False