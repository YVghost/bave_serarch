from __future__ import annotations

import re

from utils.text_norm import norm, simple_tokens, ACRONYMS_OK

# ------------------------------------------------------------------
# STOP WORDS (se descartan en tokenización)
# ------------------------------------------------------------------
_STOP = {
    "de", "del", "la", "el", "en", "y", "e", "para", "con", "a", "las", "los",
    "the", "and", "of", "in", "to", "for", "with", "an", "a",
    "mention", "mencion", "mención",
}

# ------------------------------------------------------------------
# GENÉRICAS: presencia en el perfil no discrimina (score 0)
# ------------------------------------------------------------------
_GENERIC = {
    "business", "management", "engineering", "developer", "programming",
    "legal", "health", "science", "data", "analytics", "technology",
    "administration", "administracion", "tecnico", "professional",
}

# ------------------------------------------------------------------
# PREFIJOS DE GRADO — se eliminan antes del lookup
# ------------------------------------------------------------------
_DEGREE_PREFIXES = sorted([
    "bachelor of science in", "bachelor of arts in", "bachelor of",
    "bachelor's in", "bachelor in",
    "licenciatura en", "licenciado en", "licenciada en",
    "ingenieria en", "ingenieria de", "ingeniero en", "ingeniero de",
    "maestria en", "maestría en", "master of science in", "master of arts in",
    "master of", "master's in", "master in", "masters in",
    "doctorado en", "phd in", "doctor en",
    "tecnico en", "técnico en", "técnica en",
    "profesorado en", "diplomado en", "grado en",
    "associate of", "associate in",
], key=len, reverse=True)  # más largos primero para evitar match parcial

_PARENS_RE = re.compile(r'\([^)]*\)')   # elimina (BBA), (MBA), etc.
_COMMA_RE  = re.compile(r',.*$')        # elimina ", Marketing" en "BBA, Marketing"


def _strip_degree(carrera: str) -> str:
    """
    Quita prefijos de grado y acrónimos entre paréntesis para llegar
    a la disciplina central.
    Ej: "Bachelor of Business Administration (BBA), Marketing"
        → "business administration"
    """
    c = norm(carrera)
    c = _PARENS_RE.sub(" ", c)   # quitar (BBA), (MBA)…
    c = _COMMA_RE.sub("", c)     # quitar ", Marketing" como especialización
    c = re.sub(r"\s+", " ", c).strip()

    for prefix in _DEGREE_PREFIXES:
        p = norm(prefix)
        if c.startswith(p):
            c = c[len(p):].strip()
            break

    return c


# ------------------------------------------------------------------
# MAPEOS MANUALES EXACTOS  (clave = disciplina normalizada)
# Incluye español, inglés y variantes comunes CR / EC
# ------------------------------------------------------------------
_MANUAL_EXACT: dict[str, list[str]] = {

    # --- ADMINISTRACIÓN / BUSINESS ---
    norm("administracion de empresas"): [
        "business administration", "bba", "administracion", "gestion empresarial",
        "business management", "management", "gerencia",
    ],
    norm("business administration"): [
        "administracion de empresas", "bba", "gestion empresarial",
        "business management", "gerencia", "administracion",
    ],
    norm("gestion empresarial"): [
        "business administration", "business management",
        "administracion de empresas", "gerencia",
    ],
    norm("administracion publica"): [
        "public administration", "gestion publica", "gobierno",
    ],
    norm("public administration"): [
        "administracion publica", "gestion publica",
    ],

    # --- MARKETING / MERCADEO ---
    norm("marketing"): [
        "mercadeo", "mercadotecnia", "publicidad", "advertising",
        "branding", "digital marketing", "marketing digital",
    ],
    norm("mercadeo"): [
        "marketing", "mercadotecnia", "publicidad", "advertising",
        "branding", "marketing digital",
    ],
    norm("publicidad"): [
        "advertising", "marketing", "branding", "mercadeo",
        "comunicacion publicitaria",
    ],
    norm("advertising"): [
        "publicidad", "marketing", "branding", "mercadeo",
    ],

    # --- NEGOCIOS INTERNACIONALES ---
    norm("negocios internacionales"): [
        "international business", "comercio internacional",
        "global business", "comercio exterior",
    ],
    norm("international business"): [
        "negocios internacionales", "comercio internacional",
        "global business", "comercio exterior",
    ],
    norm("comercio exterior"): [
        "international business", "negocios internacionales",
        "comercio internacional",
    ],

    # --- CONTABILIDAD / ACCOUNTING ---
    norm("contabilidad"): [
        "accounting", "cpa", "contaduria", "auditoria",
        "contabilidad y auditoria", "contabilidad y finanzas",
    ],
    norm("accounting"): [
        "contabilidad", "cpa", "contaduria", "auditoria",
    ],
    norm("contaduria publica"): [
        "accounting", "cpa", "contabilidad", "auditoria",
        "public accounting",
    ],
    norm("auditoria"): [
        "auditing", "accounting", "contabilidad", "cpa",
    ],

    # --- FINANZAS / FINANCE ---
    norm("finanzas"): [
        "finance", "financial management", "banca", "banking",
        "economia financiera", "finanzas y banca",
    ],
    norm("finance"): [
        "finanzas", "financial management", "banca", "banking",
    ],
    norm("finanzas y banca"): [
        "finance", "banking", "finanzas", "banca",
    ],

    # --- ECONOMÍA / ECONOMICS ---
    norm("economia"): [
        "economics", "economic", "economista",
    ],
    norm("economics"): [
        "economia", "economista",
    ],

    # --- RECURSOS HUMANOS / HR ---
    norm("recursos humanos"): [
        "human resources", "hr", "gestion humana", "talento humano",
        "people management", "gestión del talento",
    ],
    norm("human resources"): [
        "recursos humanos", "hr", "gestion humana", "talento humano",
    ],
    norm("gestion del talento humano"): [
        "human resources", "hr", "recursos humanos", "talento humano",
    ],

    # --- INGENIERÍA DE SOFTWARE ---
    norm("ingenieria de software"): [
        "software engineering", "software engineer", "computer science",
        "cs", "it", "developer", "desarrollo de software",
    ],
    norm("software engineering"): [
        "ingenieria de software", "computer science", "cs",
        "desarrollo de software", "developer",
    ],
    norm("desarrollo de software"): [
        "software engineering", "software developer", "computer science",
        "ingenieria de software",
    ],

    # --- SISTEMAS / INFORMÁTICA ---
    norm("ingenieria en sistemas"): [
        "information systems", "computer engineering", "sistemas informaticos",
        "it", "software engineering", "informatica",
    ],
    norm("informatica"): [
        "computer science", "it", "information technology",
        "sistemas", "software engineering",
    ],
    norm("tecnologias de informacion"): [
        "information technology", "it", "informatica",
        "sistemas", "technology management",
    ],
    norm("information technology"): [
        "tecnologias de informacion", "informatica", "it",
        "sistemas informaticos",
    ],
    norm("computer science"): [
        "informatica", "ingenieria de software", "cs",
        "ciencias de la computacion", "sistemas",
    ],

    # --- INGENIERÍA CIVIL ---
    norm("ingenieria civil"): [
        "civil engineering", "construccion", "obras civiles",
    ],
    norm("civil engineering"): [
        "ingenieria civil", "construccion",
    ],

    # --- INGENIERÍA INDUSTRIAL ---
    norm("ingenieria industrial"): [
        "industrial engineering", "ingenieria de produccion",
        "produccion", "manufactura",
    ],
    norm("industrial engineering"): [
        "ingenieria industrial", "produccion", "manufactura",
    ],

    # --- INGENIERÍA MECÁNICA ---
    norm("ingenieria mecanica"): [
        "mechanical engineering", "mecanica",
    ],
    norm("mechanical engineering"): [
        "ingenieria mecanica", "mecanica",
    ],

    # --- INGENIERÍA ELÉCTRICA / ELECTRÓNICA ---
    norm("ingenieria electrica"): [
        "electrical engineering", "electronica", "electrica",
    ],
    norm("ingenieria electronica"): [
        "electronics engineering", "electronica", "electrical engineering",
    ],
    norm("electrical engineering"): [
        "ingenieria electrica", "ingenieria electronica", "electronica",
    ],

    # --- INGENIERÍA QUÍMICA ---
    norm("ingenieria quimica"): [
        "chemical engineering", "quimica industrial",
    ],
    norm("chemical engineering"): [
        "ingenieria quimica", "quimica industrial",
    ],

    # --- INGENIERÍA AMBIENTAL ---
    norm("ingenieria ambiental"): [
        "environmental engineering", "medio ambiente", "gestion ambiental",
    ],
    norm("environmental engineering"): [
        "ingenieria ambiental", "gestion ambiental",
    ],

    # --- ARQUITECTURA ---
    norm("arquitectura"): [
        "architecture", "arquitecto", "diseño arquitectonico", "urban design",
    ],
    norm("architecture"): [
        "arquitectura", "arquitecto", "diseño arquitectonico",
    ],

    # --- DISEÑO ---
    norm("diseno grafico"): [
        "graphic design", "diseñador grafico", "diseño visual",
        "visual design", "branding",
    ],
    norm("graphic design"): [
        "diseno grafico", "diseñador grafico", "diseño visual",
    ],
    norm("diseno de interiores"): [
        "interior design", "interior designer", "diseño interior",
    ],
    norm("interior design"): [
        "diseno de interiores", "interior designer",
    ],
    norm("diseno industrial"): [
        "industrial design", "product design",
    ],

    # --- COMUNICACIÓN ---
    norm("comunicacion"): [
        "communications", "communication", "periodismo", "journalism",
        "relaciones publicas", "public relations", "pr",
    ],
    norm("periodismo"): [
        "journalism", "comunicacion", "media", "prensa",
    ],
    norm("relaciones publicas"): [
        "public relations", "pr", "comunicacion", "communications",
    ],
    norm("communications"): [
        "comunicacion", "periodismo", "journalism", "relaciones publicas",
    ],

    # --- DERECHO / LAW ---
    norm("derecho"): [
        "law", "legal", "jurisprudencia", "abogado", "abogacia",
        "ciencias juridicas", "attorney",
    ],
    norm("law"): [
        "derecho", "legal", "jurisprudencia", "abogado", "attorney",
    ],
    norm("laws"): [
        "derecho", "law", "legal", "jurisprudencia", "abogado", "attorney",
    ],
    norm("ciencias juridicas"): [
        "law", "derecho", "legal", "jurisprudencia",
    ],

    # --- PSICOLOGÍA ---
    norm("psicologia"): [
        "psychology", "psicologo", "psicologia clinica",
        "psicologia organizacional", "counseling",
    ],
    norm("psychology"): [
        "psicologia", "psicologo", "counseling",
    ],

    # --- MEDICINA Y SALUD ---
    norm("medicina"): [
        "medicine", "medical", "medico", "doctor", "md",
    ],
    norm("medicine"): [
        "medicina", "medico", "doctor", "md",
    ],
    norm("enfermeria"): [
        "nursing", "nurse", "enfermero",
    ],
    norm("nursing"): [
        "enfermeria", "nurse", "enfermero",
    ],
    norm("odontologia"): [
        "dentistry", "dental", "odontologo", "dentist",
    ],
    norm("dentistry"): [
        "odontologia", "dental", "odontologo", "dentist",
    ],
    norm("nutricion"): [
        "nutrition", "dietetics", "nutricionista", "dietitian",
    ],
    norm("nutrition"): [
        "nutricion", "dietetics", "nutricionista",
    ],
    norm("farmacia"): [
        "pharmacy", "pharmaceutical", "farmaceutico",
    ],
    norm("salud publica"): [
        "public health", "salud comunitaria", "epidemiologia",
    ],

    # --- EDUCACIÓN ---
    norm("educacion"): [
        "education", "teaching", "docencia", "pedagogy", "pedagogia",
        "enseñanza",
    ],
    norm("education"): [
        "educacion", "teaching", "docencia", "pedagogia",
    ],
    norm("educacion primaria"): [
        "elementary education", "primary education", "enseñanza primaria",
    ],
    norm("educacion preescolar"): [
        "early childhood education", "preescolar",
    ],

    # --- TRABAJO SOCIAL ---
    norm("trabajo social"): [
        "social work", "social worker", "desarrollo social",
    ],
    norm("social work"): [
        "trabajo social", "social worker",
    ],

    # --- TURISMO / HOTELERÍA ---
    norm("turismo"): [
        "tourism", "hospitality", "hoteleria", "turismo y hoteleria",
        "travel", "hotel management",
    ],
    norm("hoteleria"): [
        "hospitality", "hotel management", "turismo",
    ],
    norm("tourism"): [
        "turismo", "hospitality", "hoteleria",
    ],

    # --- LOGÍSTICA / SUPPLY CHAIN ---
    norm("logistica"): [
        "logistics", "supply chain", "cadena de suministro",
        "operaciones", "operations",
    ],
    norm("logistics"): [
        "logistica", "supply chain", "cadena de suministro",
    ],

    # --- GASTRONOMÍA ---
    norm("gastronomia"): [
        "gastronomy", "culinary arts", "cocina", "chef",
    ],

    # --- BIOLOGÍA / CIENCIAS ---
    norm("biologia"): [
        "biology", "biologo", "ciencias biologicas",
    ],
    norm("quimica"): [
        "chemistry", "quimico",
    ],
}

# ------------------------------------------------------------------
# FAMILIAS — expansión secundaria cuando se detecta un token clave
# ------------------------------------------------------------------
_FAMILY_EQUIV: dict[str, list[str]] = {
    "software":        ["software engineering", "computer science", "it", "developer", "sistemas"],
    "administracion":  ["business administration", "management", "business", "gestion"],
    "negocios":        ["business", "international business", "trade", "comercio"],
    "marketing":       ["mercadeo", "advertising", "branding", "marketing digital"],
    "mercadeo":        ["marketing", "advertising", "branding"],
    "publicidad":      ["advertising", "marketing", "branding"],
    "diseno":          ["design", "graphic design", "interior design", "product design"],
    "design":          ["diseno", "diseño", "grafico", "graphic"],
    "derecho":         ["law", "legal", "jurisprudencia"],
    "law":             ["derecho", "legal"],
    "comunicacion":    ["communications", "journalism", "pr", "media"],
    "finanzas":        ["finance", "financial", "banca", "banking"],
    "finance":         ["finanzas", "banca", "contabilidad"],
    "contabilidad":    ["accounting", "cpa", "auditoria"],
    "accounting":      ["contabilidad", "cpa", "auditoria"],
    "sistemas":        ["information systems", "it", "informatica", "computer"],
    "ingenieria":      ["engineering"],
    "engineering":     ["ingenieria"],
    "psicologia":      ["psychology", "counseling"],
    "psychology":      ["psicologia", "counseling"],
    "educacion":       ["education", "teaching", "docencia"],
    "education":       ["educacion", "teaching", "docencia"],
    "logistica":       ["logistics", "supply chain", "operaciones"],
    "recursos":        ["human resources", "hr", "talento humano"],
    "turismo":         ["tourism", "hospitality", "hoteleria"],
    "medicina":        ["medicine", "medical", "md"],
    "enfermeria":      ["nursing", "nurse"],
    "salud":           ["health", "medicine", "nursing", "medical"],
}


# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------

def _clean_tokens(s: str) -> list[str]:
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


# ------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------

def expand_carrera_keywords(carrera: str) -> list[str]:
    """
    Retorna lista de keywords para matching en título/snippet de LinkedIn.

    Maneja tanto español como inglés y nombres con prefijos de grado:
      "Bachelor of Business Administration (BBA), Marketing"
      → mismo resultado que "Administración de Empresas" + "Marketing"
    """
    c_norm = norm(carrera)
    stripped = _strip_degree(carrera)   # versión sin prefijo de grado

    out: list[str] = []

    # 1. Carrera completa original
    if c_norm:
        out.append(c_norm)

    # 2. Disciplina sin prefijo de grado (si es distinta)
    if stripped and stripped != c_norm:
        out.append(stripped)

    # 3. Lookup exacto — intentar con original y con stripped
    for key in (c_norm, stripped):
        if key in _MANUAL_EXACT:
            out.extend(_MANUAL_EXACT[key])

    # 4. Tokens de la carrera (con y sin prefijo)
    ts_orig    = _clean_tokens(carrera)
    ts_stripped = _clean_tokens(stripped)
    all_tokens = _dedup_preserve(ts_orig + ts_stripped)
    out.extend(all_tokens)

    # 5. Expansión por familias
    for t in all_tokens:
        if t in _FAMILY_EQUIV:
            out.extend(_FAMILY_EQUIV[t])

    # 6. Si hay token "ingenieria"/"engineering", agregar su par
    if "ingenieria" in all_tokens:
        out.append("engineering")
    if "engineering" in all_tokens:
        out.append("ingenieria")

    return _dedup_preserve(out)


def is_generic_kw(kw: str) -> bool:
    return norm(kw) in _GENERIC
