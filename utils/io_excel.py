import pandas as pd

DEFAULT_EXTRA_COLS = {
    "Score": "",
    "Confianza": "",
    "Estudia_o_Estudio": "",
    "Candidatos_Top3": "",
    "Match_UDLA": "",
    "Match_Carrera": "",
}

def read_lote(path: str) -> pd.DataFrame:
    return pd.read_excel(path)

def ensure_columns(df: pd.DataFrame, cols_defaults: dict = None) -> pd.DataFrame:
    cols_defaults = cols_defaults or DEFAULT_EXTRA_COLS
    for col, default in cols_defaults.items():
        if col not in df.columns:
            df[col] = default
    return df

def write_lote(df: pd.DataFrame, path: str) -> None:
    df.to_excel(path, index=False)
