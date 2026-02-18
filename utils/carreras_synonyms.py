from utils.text_norm import norm, simple_tokens

_STOP = {
    "de","del","la","el","en","y","e","para","con","a",
    "the","and","of","in","to","for","mention","mencion","mención"
}

def _clean_tokens(s: str) -> list[str]:
    ts = [t for t in simple_tokens(s) if t not in _STOP and len(t) >= 3]
    return ts

def _dedup_preserve(seq: list[str]) -> list[str]:
    out, seen = [], set()
    for x in seq:
        x = norm(x)
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out

def tipo_programa(carrera: str) -> str:
    """
    Clasifica (simple) según texto:
      - MAESTRIA
      - ESPECIALIZACION
      - GRADO (default)
    """
    c = norm(carrera)
    if "maestr" in c or c.startswith("master"):
        return "MAESTRIA"
    if "especializ" in c:
        return "ESPECIALIZACION"
    return "GRADO"

# --- MAPEOS MANUALES (exactos) para las carreras/maestrías que vienen en tu Excel ---
# Clave: carrera normalizada completa
# Valor: lista de equivalencias ES/EN + formas abreviadas que pueden aparecer en snippets
_MANUAL_EXACT = {
    # Grado
    norm("MEDICINA"): ["medicine", "medical doctor", "md"],
    norm("MEDICINA VETERINARIA"): ["veterinary", "veterinary medicine", "dvm", "vet"],
    norm("ODONTOLOGIA"): ["dentistry", "dental", "odontology"],
    norm("ENFERMERIA"): ["nursing", "nurse"],
    norm("FISIOTERAPIA"): ["physiotherapy", "physical therapy", "pt"],
    norm("PSICOLOGIA"): ["psychology", "psychologist"],
    norm("ARQUITECTURA"): ["architecture", "architect"],
    norm("DISEÑO GRAFICO"): ["graphic design", "visual design", "designer"],
    norm("DISEÑO GRAFICO Y COMUNICACION VISUAL"): ["graphic design", "visual communication", "visual design"],
    norm("DISEÑO DE INTERIORES"): ["interior design", "interior designer"],
    norm("DISEÑO DE PRODUCTOS"): ["product design", "industrial design", "design engineering"],
    norm("PUBLICIDAD"): ["advertising", "ads", "publicidad y marketing"],
    norm("COMUNICACION"): ["communication", "communications", "corporate communication"],
    norm("PERIODISMO"): ["journalism", "reporter"],
    norm("MULTIMEDIA Y PRODUCCION AUDIOVISUAL"): ["multimedia", "audiovisual production", "film production", "video production"],
    norm("SONIDO Y ACÚSTICA"): ["sound", "acoustics", "audio engineering", "sound engineering"],
    norm("ARTES MUSICALES"): ["music", "musical arts", "musician"],
    norm("CINE"): ["film", "cinema", "filmmaking"],
    norm("TURISMO"): ["tourism", "travel"],
    norm("HOSPITALIDAD Y HOTELERÍA"): ["hospitality", "hotel management", "hoteleria", "tourism"],
    norm("GASTRONOMIA"): ["gastronomy", "culinary", "chef", "cuisine"],
    norm("AGROINDUSTRIA"): ["agroindustry", "food industry", "agribusiness"],
    norm("ECONOMIA"): ["economics", "economist"],
    norm("FINANZAS"): ["finance", "financial", "finanzas corporativas", "banking"],
    norm("ADMINISTRACION DE EMPRESAS"): ["business administration", "business", "management", "mba track"],
    norm("NEGOCIOS INTERNACIONALES"): ["international business", "global business", "trade", "comercio internacional"],
    norm("RELACIONES INTERNACIONALES"): ["international relations", "ir", "political science"],
    norm("DERECHO"): ["law", "legal", "jurisprudence"],
    norm("INGENIERIA DE SOFTWARE"): ["software engineering", "software engineer", "computer science", "cs", "it", "developer"],
    norm("INGENIERÍA INDUSTRIAL"): ["industrial engineering", "operations", "process engineering"],
    norm("INGENIERÍA AMBIENTAL"): ["environmental engineering", "environmental", "sustainability"],
    norm("TELECOMUNICACIONES"): ["telecommunications", "telecom", "networking", "networks"],
    norm("ELECTRÓNICA Y AUTOMATIZACIÓN"): ["electronics", "automation", "mechatronics", "control systems", "industrial automation"],
    norm("BIOTECNOLOGÍA"): ["biotechnology", "biotech"],

    # Maestrías (varias)
    norm("MAESTRÍA EN SEGURIDAD Y SALUD OCUPACIONAL"): [
        "occupational health", "occupational safety", "hse", "sso", "osh", "workplace safety"
    ],
    norm("MAESTRÍA EN SALUD PÚBLICA"): ["public health", "mph"],
    norm("MAESTRÍA EN GERENCIA INSTITUCIONES DE SALUD"): [
        "healthcare management", "health management", "hospital management", "gerencia en salud"
    ],
    norm("MAESTRÍA EN PSICOLOGÍA CLÍNICA"): ["clinical psychology", "psicologia clinica"],
    norm("MAESTRÍA EN PSICOTERAPIA"): ["psychotherapy", "therapy"],
    norm("MAESTRÍA EN  NEUROPSICOLOGÍA CLÍNICA"): ["clinical neuropsychology", "neuropsychology"],
    norm("MAESTRÍA EN\u00A0 NEUROPSICOLOGÍA CLÍNICA"): ["clinical neuropsychology", "neuropsychology"],  # por NBSP
    norm("MAESTRÍA EN NUTRICIÓN Y DIETÉTICA"): ["nutrition", "dietetics", "clinical nutrition"],
    norm("MAESTRÍA DE ENFERMERÍA"): ["nursing", "advanced nursing", "master of nursing"],
    norm("MAESTRÍA EN TERAPIA MANUAL ORTOPÉDICA INTEGRAL"): [
        "manual therapy", "orthopedic manual therapy", "omt"
    ],
    norm("MAESTRÍA EN TERAPIA RESPIRATORIA"): ["respiratory therapy"],
    norm("MAESTRÍA EN NEURORREHABILITACIÓN"): ["neurorehabilitation", "rehabilitation"],
    norm("MAESTRÍA EN GESTIÓN DE PROYECTOS"): ["project management", "pmp"],
    norm("MAESTRÍA EN LOGÍSTICA Y CADENA DE SUMINISTRO"): ["logistics", "supply chain", "scm"],
    norm("MAESTRÍA EN INTELIGENCIA DE NEGOCIOS Y CIENCIA DE DATOS"): [
        "business intelligence", "data science", "analytics", "bi", "ds"
    ],
    norm("MAESTRÍA EN GESTIÓN POR PROCESOS CON MENCIÓN EN TRANSFORMACIÓN DIGITAL"): [
        "process management", "business process", "digital transformation", "bpm"
    ],
    norm("MAESTRÍA EN GERENCIA TRIBUTARIA"): ["tax management", "taxation", "tributaria"],
    norm("MAESTRÍA EN DIRECCIÓN Y POSTPRODUCCIÓN AUDIOVISUAL DIGITAL"): [
        "audiovisual", "postproduction", "post-production", "video editing"
    ],
    norm("MAESTRÍA EN COMUNICACIÓN POLÍTICA"): ["political communication"],
    norm("MAESTRÍA EN DIRECCIÓN DE EMPRESAS"): ["business management", "executive management", "management"],
    norm("MAESTRIA EN ADMINISTRACION DE EMPRESAS MENCION EN GERENCIA ORGANIZACIONAL"): [
        "business administration", "organizational management", "management"
    ],
    norm("MAESTRÍA EN ADMINISTRACIÓN DE EMPRESAS"): ["business administration", "mba", "management"],
    norm("MAESTRIA EN MERCADOTECNIA MENCION EN GERENCIA DE MARCA"): [
        "marketing", "brand management", "branding"
    ],
    norm("MAESTRIA EN MERCADOTECNIA CON MENCION EN ESTRATEGIA DIGITAL"): [
        "digital marketing", "marketing strategy", "growth marketing"
    ],
    norm("MAESTRÍA EN DERECHO DIGITAL E INNOVACIÓN CON MENCIÓN EN ECONOMÍA CONFIANZA Y TRANSFORMACIÓN DIGITAL"): [
        "digital law", "law and technology", "legal tech", "innovation law"
    ],
    norm("MAESTRÍA EN DERECHO PROCESAL CONSTITUCIONAL"): ["constitutional law", "procedural law"],
    norm("MASTER EN DERECHO PENAL CON MENCIÓN EN CRIMINALIDAD COMPLEJA"): ["criminal law", "penal", "criminology"],
    norm("MAESTRÍA EN FILOSOFÍA, POLÍTICA Y ECONOMÍA"): ["philosophy", "politics", "economics", "ppe"],
    norm("MAESTRÍA EN ARBITRAJE COMERCIAL Y DE INVERSIONES"): ["arbitration", "commercial arbitration", "investment arbitration"],
    norm("MAESTRÍA EN LIDERAZGO EDUCATIVO"): ["educational leadership"],
    norm("MAESTRÍA EN URBANISMO"): ["urbanism", "urban planning"],
    norm("MAESTRIA EN DIRECCION DE OPERACIONES Y SEGURIDAD INDUSTRIAL"): [
        "operations management", "industrial safety", "hse", "sso", "osh"
    ],
    norm("MAESTRIA FINANZAS MENCION MERCADO DE VALORES Y BANCA"): [
        "finance", "capital markets", "banking", "investment"
    ],
    norm("MAESTRIA EN DESARROLLO E INNOVACION DE ALIMENTOS"): [
        "food innovation", "food development", "food science"
    ],

    # Especializaciones
    norm("ESPECIALIZACION MEDICA EN ORTODONCIA"): ["orthodontics", "orthodontist", "ortodoncia"],
}

# --- FAMILIAS (para cuando no haya match exacto, o el texto venga incompleto) ---
_FAMILY_EQUIV = {
    "software": ["software engineering", "computer science", "it", "developer", "programming", "informatics"],
    "ingenieria": ["engineering"],
    "industrial": ["industrial engineering", "operations", "process engineering"],
    "ambiental": ["environmental", "sustainability"],
    "electronica": ["electronics", "automation", "control systems"],
    "automatizacion": ["automation", "industrial automation", "control systems"],
    "telecomunicaciones": ["telecommunications", "networking", "networks", "telecom"],
    "biotecnologia": ["biotechnology", "biotech"],
    "medicina": ["medicine", "medical", "md"],
    "veterinaria": ["veterinary", "dvm", "vet"],
    "odontologia": ["dentistry", "dental"],
    "enfermeria": ["nursing"],
    "fisioterapia": ["physiotherapy", "physical therapy"],
    "psicologia": ["psychology"],
    "neuropsicologia": ["neuropsychology"],
    "nutricion": ["nutrition", "dietetics"],
    "salud": ["health", "public health", "healthcare"],
    "seguridad": ["occupational safety", "hse", "osh", "sso"],
    "ocupacional": ["occupational health", "occupational safety", "hse", "osh"],
    "logistica": ["logistics", "supply chain", "scm"],
    "cadena": ["supply chain", "scm"],
    "proyectos": ["project management", "pmp"],
    "inteligencia": ["business intelligence", "analytics", "data science", "bi"],
    "datos": ["data science", "analytics"],
    "mercadotecnia": ["marketing", "brand management", "digital marketing"],
    "marketing": ["marketing", "digital marketing", "branding"],
    "finanzas": ["finance", "banking", "capital markets"],
    "economia": ["economics", "finance"],
    "administracion": ["business administration", "management", "business"],
    "negocios": ["business", "international business", "trade"],
    "derecho": ["law", "legal"],
    "penal": ["criminal law", "criminology"],
    "arbitraje": ["arbitration"],
    "arquitectura": ["architecture"],
    "urbanismo": ["urban planning", "urbanism"],
    "diseno": ["design", "graphic design", "interior design", "product design"],
    "publicidad": ["advertising", "marketing"],
    "comunicacion": ["communication", "journalism", "public relations"],
    "periodismo": ["journalism"],
    "audiovisual": ["audiovisual", "multimedia", "film", "video production"],
    "multimedia": ["multimedia", "audiovisual"],
    "sonido": ["audio engineering", "sound", "acoustics"],
    "acustica": ["acoustics", "audio engineering"],
    "cine": ["film", "cinema"],
    "turismo": ["tourism", "hospitality"],
    "hospitalidad": ["hospitality", "hotel management"],
    "gastronomia": ["culinary", "gastronomy"],
    "agroindustria": ["agribusiness", "agroindustry", "food industry"],
    "educacion": ["education", "educational leadership"],
}

def expand_carrera_keywords(carrera: str) -> list[str]:
    """
    Devuelve keywords para matching en snippets/títulos.
    - 1) intenta match exacto de la carrera del Excel
    - 2) agrega tokens limpios
    - 3) expande por familias detectadas
    """
    c_norm = norm(carrera)
    out = []

    # carrera completa
    if c_norm:
        out.append(c_norm)

    # exact manual
    if c_norm in _MANUAL_EXACT:
        out.extend(_MANUAL_EXACT[c_norm])

    # tokens
    ts = _clean_tokens(carrera)
    out.extend(ts)

    # familias
    for t in ts:
        if t in _FAMILY_EQUIV:
            out.extend(_FAMILY_EQUIV[t])

    # ingeniería -> engineering
    if "ingenieria" in ts:
        out.append("engineering")

    # dedup
    return _dedup_preserve(out)
