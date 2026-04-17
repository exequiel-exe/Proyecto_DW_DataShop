import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 1 – ALMACENES (STG)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS 
# ============================================================

# Ruta base según tu mensaje
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Ruta exacta de los archivos originales
ORIG_PATH = os.path.join(BASE_PATH, "01_RAW", "Datos_Originales")
# Ruta para guardar los CSV limpios (QA) dentro de la carpeta STG
QA_PATH = os.path.join(BASE_PATH, "02_STG", "Archivos_Limpios")

server_name = r"DESKTOP-TPLKQNF\SQLSERVEREXP2019"
engine = create_engine(
    f"mssql+pyodbc://{server_name}/DW_DataShop?"
    "driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
)

# ============================================================
# 2. EXTRACT
# ============================================================

csv_path = os.path.join(ORIG_PATH, "Almacenes.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se encontró el archivo en {csv_path}")
    exit()

# Usamos utf-8-sig por si el CSV viene de Excel con BOM
df = pd.read_csv(csv_path, encoding="utf-8-sig")
print("EXTRACT OK – Registros originales:", len(df))

# ============================================================
# 3. TRANSFORM
# ============================================================

# Eliminar filas completamente vacías
df.dropna(how="all", inplace=True)

# Limpiar espacios en blanco en todos los campos
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Asegurar que CodAlmacen sea numérico y no nulo
df["CodAlmacen"] = pd.to_numeric(df["CodAlmacen"], errors="coerce")
df = df[df["CodAlmacen"].notna()]
df["CodAlmacen"] = df["CodAlmacen"].astype(int)

# Normalizar texto (Nombres propios)
text_cols = ["Nombre_Almacen", "Ubicacion"]
for col in text_cols:
    df[col] = df[col].astype(str).str.title()

# Eliminar duplicados por la clave de almacén
df.drop_duplicates(subset="CodAlmacen", inplace=True)

print("TRANSFORM OK – Registros para cargar:", len(df))

# ============================================================
# 4. LOAD – STAGING (Hacia la tabla que creaste por SQL)
# ============================================================

try:
    # IMPORTANTE: if_exists="append" para respetar tu CREATE TABLE de SQL
    df.to_sql(
        "STG_Dim_Almacenes", 
        engine, 
        if_exists="append", 
        index=False
    )
    print("LOAD OK – Datos inyectados en STG_Dim_Almacenes")
except Exception as e:
    print("❌ ERROR al cargar en SQL Server:", e)

# ============================================================
# 5. EXPORT CSV LIMPIO (Para control visual)
# ============================================================

os.makedirs(QA_PATH, exist_ok=True)
csv_limpio_path = os.path.join(QA_PATH, "Almacenes_limpio.csv")
df.to_csv(csv_limpio_path, index=False, encoding="utf-8-sig")

print("QA OK – CSV de control guardado en:", csv_limpio_path)
print("====================================")