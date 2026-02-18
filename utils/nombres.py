from utils.text_norm import norm

def variantes_nombres(nombre_completo: str, max_variantes: int = 3) -> list[str]:
    """
    Variantes:
      1) N1 Ap1
      2) N1 N2 Ap1
      3) N2 Ap1
    Asume comúnmente: Ap1 Ap2 N1 N2...
    """
    parts = [p.strip() for p in str(nombre_completo or "").split() if p.strip()]
    if len(parts) == 0:
        return []

    # Si ya viene como "Nombre Apellido" (2 partes), usa eso.
    if len(parts) == 2:
        return [f"{parts[0]} {parts[1]}"]

    # Heurística principal: 2 apellidos + nombres
    ap1 = parts[0]
    ap2 = parts[1] if len(parts) >= 2 else ""
    nombres = parts[2:] if len(parts) >= 3 else parts[1:]

    n1 = nombres[0] if len(nombres) >= 1 else ""
    n2 = nombres[1] if len(nombres) >= 2 else ""

    cand = []
    if n1 and ap1:
        cand.append(f"{n1} {ap1}")
    if n1 and n2 and ap1:
        cand.append(f"{n1} {n2} {ap1}")
    if n2 and ap1:
        cand.append(f"{n2} {ap1}")

    # fallback si quedó corto
    if len(cand) < 2 and n1 and ap2:
        cand.append(f"{n1} {ap2}")

    # dedup por normalización
    out = []
    seen = set()
    for c in cand:
        c = " ".join(c.split()).strip()
        k = norm(c)
        if c and k not in seen:
            out.append(c)
            seen.add(k)

    return out[:max_variantes]
