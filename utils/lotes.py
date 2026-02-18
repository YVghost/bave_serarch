from pathlib import Path
import pandas as pd

def dividir_lotes_excel(
    input_path: str,
    output_dir: str,
    batch_size: int = 1000,
    prefix: str = "estudiantes_lote_",
    start_index: int = 1
) -> list[str]:
    df = pd.read_excel(input_path)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    lote = 0
    for i in range(0, len(df), batch_size):
        lote += 1
        chunk = df.iloc[i:i+batch_size].copy()
        fname = f"{prefix}{(start_index + lote - 1):03d}.xlsx"
        fpath = out_dir / fname
        chunk.to_excel(fpath, index=False)
        paths.append(str(fpath))

    return paths
