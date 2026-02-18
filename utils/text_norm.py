import re
from unidecode import unidecode


def tokens(s: str) -> list[str]:
    """Tokeniza conservando solo letras/números (simple)."""
    s = norm(s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return [t for t in s.split(" ") if t]

def contains_any(haystack: str, needles: list[str]) -> bool:
    h = norm(haystack)
    return any(norm(n) in h for n in needles if n)

def count_hits(haystack: str, needles: list[str]) -> int:
    h = norm(haystack)
    return sum(1 for n in needles if n and norm(n) in h)

def norm(s: str) -> str:
    """
    Normaliza:
    - convierte a str
    - quita tildes
    - lower
    - limpia espacios raros (incluye NBSP)
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\u00A0", " ")   # NBSP
    s = s.replace("\u200B", "")   # zero-width
    s = unidecode(s).lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_keep_symbols(s: str) -> str:
    """Como norm pero no elimina signos; útil si quieres ver texto original normalizado."""
    return norm(s)

def simple_tokens(s: str) -> list[str]:
    s = norm(s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return [t for t in s.split() if t]

