from __future__ import annotations

from pathlib import Path
import pandas as pd


def dividir_lotes_excel(
    input_path: str,
    output_dir: str,
    batch_size: int = 1000,
    prefix: str = "lote_",
    start_index: int = 1,
) -> list[str]:
    """
    Divide un archivo Excel en lotes de 'batch_size' filas.
    No asume nada sobre los nombres de columnas: pasa todo tal cual.
    """
    df = pd.read_excel(input_path, dtype=str)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for lote, i in enumerate(range(0, len(df), batch_size), start=1):
        chunk = df.iloc[i : i + batch_size].copy()
        fname = f"{prefix}{(start_index + lote - 1):03d}.xlsx"
        fpath = out_dir / fname
        chunk.to_excel(fpath, index=False)
        paths.append(str(fpath))

    return paths


def procesar_dividir(base_dir: str, batch_size: int = 1000) -> int:
    """
    Escanea {base_dir}/dividir/ en busca de cualquier archivo .xlsx o .xls,
    divide cada uno en lotes de 'batch_size' filas y los deposita en
    {base_dir}/lotes/.

    El prefijo de cada lote es el nombre del archivo fuente sin extensión.
    Retorna el total de archivos de lote generados.

    Si no hay carpeta 'dividir' o está vacía, no hace nada.
    """
    dividir_dir = Path(base_dir) / "dividir"
    lotes_dir = Path(base_dir) / "lotes"

    if not dividir_dir.exists():
        return 0

    archivos = sorted(dividir_dir.glob("*.xlsx")) + sorted(dividir_dir.glob("*.xls"))
    if not archivos:
        return 0

    lotes_dir.mkdir(parents=True, exist_ok=True)
    total = 0

    for archivo in archivos:
        prefix = archivo.stem + "_lote_"

        # Si ya hay lotes generados para este archivo, no re-dividir
        ya_existen = list(lotes_dir.glob(f"{prefix}*.xlsx"))
        if ya_existen:
            print(f"  [dividir] {archivo.name} → ya dividido ({len(ya_existen)} lote(s)), se omite")
            continue

        generados = dividir_lotes_excel(
            input_path=str(archivo),
            output_dir=str(lotes_dir),
            batch_size=batch_size,
            prefix=prefix,
        )
        print(f"  [dividir] {archivo.name} → {len(generados)} lote(s) generado(s)")
        total += len(generados)

    return total
