import pandas as pd
import os

# Carpeta donde están los excels
carpeta = r"C:\Users\dddma\OneDrive\Escritorio\Codigos_repors\bave_serarch\data\output"

# Lista para guardar los dataframes
dfs = []

# Recorrer archivos
for archivo in os.listdir(carpeta):
    if archivo.endswith(".xlsx"):
        ruta = os.path.join(carpeta, archivo)
        
        try:
            df = pd.read_excel(ruta)
            dfs.append(df)
            print(f"Leído: {archivo} ({len(df)} filas)")
        except Exception as e:
            print(f"Error con {archivo}: {e}")

# Unir todos
df_final = pd.concat(dfs, ignore_index=True)

# Guardar consolidado
salida = os.path.join(carpeta, "estudiantes_consolidado.xlsx")
df_final.to_excel(salida, index=False)

print(f"\nArchivo final creado: {salida}")
print(f"Total registros: {len(df_final)}")