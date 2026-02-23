from utils.text_norm import norm


# Partículas comunes en apellidos compuestos
APELLIDO_PARTICULAS = {
    "de", "del", "de la", "de los", "de las",
    "van", "von", "da", "dos"
}

# Nombres compuestos frecuentes en LATAM
NOMBRES_COMPUESTOS = {
    "maria del carmen",
    "maria jose",
    "maria fernanda",
    "maria alejandra",
    "ana maria",
    "juan carlos",
    "jose luis",
    "jose antonio",
    "luis fernando"
}


def unir_particulas_apellido(parts: list[str]) -> list[str]:
    """
    Une partículas tipo:
    de la Cruz -> delaCruz (temporal)
    """
    resultado = []
    i = 0

    while i < len(parts):
        p = parts[i].lower()

        # intenta detectar estructuras como "de la"
        if i + 2 < len(parts) and f"{p} {parts[i+1].lower()}" in APELLIDO_PARTICULAS:
            compuesto = f"{parts[i]} {parts[i+1]} {parts[i+2]}"
            resultado.append(compuesto)
            i += 3
        elif p in APELLIDO_PARTICULAS and i + 1 < len(parts):
            compuesto = f"{parts[i]} {parts[i+1]}"
            resultado.append(compuesto)
            i += 2
        else:
            resultado.append(parts[i])
            i += 1

    return resultado


def detectar_nombre_compuesto(nombres: list[str]) -> list[str]:
    """
    Detecta si los primeros nombres forman un compuesto común.
    """
    if len(nombres) >= 2:
        candidato = f"{nombres[0]} {nombres[1]}".lower()
        if candidato in NOMBRES_COMPUESTOS:
            return [candidato] + nombres[2:]

    return nombres


def variantes_nombres(nombre_completo: str, max_variantes: int = 4) -> list[str]:
    """
    Variantes robustas:
      - Respeta apellidos compuestos
      - Respeta nombres compuestos
      - Genera combinaciones útiles para búsqueda LinkedIn
    """

    parts = [p.strip() for p in str(nombre_completo or "").split() if p.strip()]
    if not parts:
        return []

    parts = unir_particulas_apellido(parts)

    # Caso simple: Nombre Apellido
    if len(parts) == 2:
        return [f"{parts[0]} {parts[1]}"]

    # Asumimos: Ap1 Ap2 N1 N2...
    ap1 = parts[0]
    ap2 = parts[1] if len(parts) >= 2 else ""
    nombres = parts[2:] if len(parts) >= 3 else parts[1:]

    nombres = detectar_nombre_compuesto(nombres)

    n1 = nombres[0] if len(nombres) >= 1 else ""
    n2 = nombres[1] if len(nombres) >= 2 else ""

    candidatos = []

    if n1 and ap1:
        candidatos.append(f"{n1} {ap1}")

    if n1 and n2 and ap1:
        candidatos.append(f"{n1} {n2} {ap1}")

    if n2 and ap1:
        candidatos.append(f"{n2} {ap1}")

    if n1 and ap2:
        candidatos.append(f"{n1} {ap2}")

    if n1 and n2 and ap2:
        candidatos.append(f"{n1} {n2} {ap2}")

    # Deduplicación por normalización
    out = []
    seen = set()

    for c in candidatos:
        c = " ".join(c.split()).strip()
        k = norm(c)
        if c and k not in seen:
            out.append(c)
            seen.add(k)

    return out[:max_variantes]