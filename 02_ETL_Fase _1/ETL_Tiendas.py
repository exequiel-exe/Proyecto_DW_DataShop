import os
import pandas as pd
from sqlalchemy import create_engine
import logging

print("====================================")
print("ETL 1 – TIENDA (STG)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Nueva ruta de archivos originales
ORIG_PATH = os.path.join(BASE_PATH, "01_RAW", "Datos_Originales")
csv_path = os.path.join(ORIG_PATH, "Tiendas.csv")

# Rutas para logs y QA
STG_PATH = os.path.join(BASE_PATH, "02_STG")
QA_PATH = os.path.join(STG_PATH, "Archivos_Limpios")
os.makedirs(QA_PATH, exist_ok=True)

logging.basicConfig(filename=os.path.join(STG_PATH, 'etl_tiendas_errors.log'), 
                    level=logging.WARNING, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# ============================================================
# 2. EXTRACT
# ============================================================
try:
    # Forzamos CP y CodTienda a string para evitar decimales indeseados
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={"CP": str, "CodTienda": str})
    print(f"Archivo leído correctamente. Registros: {len(df)}")
except Exception as e:
    print(f"ERROR al leer el archivo: {e}")
    exit()

# ============================================================
# 3. TRANSFORM
# ============================================================

# Renombrar columnas según tu CREATE TABLE
df.columns = ["CodTienda", "Descripcion", "Direccion", "Localidad", "Provincia", "CP", "TipoTienda"]

# Eliminar filas vacías
df.dropna(how="all", inplace=True)

# Limpiar espacios
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Reemplazar vacíos por NULL
df.replace({"": None, " ": None}, inplace=True)

# --- LIMPIEZA DE CP ---
def limpiar_cp(cp):
    if pd.isna(cp) or str(cp).lower() in ['nan', '']:
        return None
    # Quitamos el .0 si Pandas lo interpretó como float
    c = str(cp).split('.')[0]
    return c.zfill(4)

df["CP"] = df["CP"].apply(limpiar_cp)

# --- NORMALIZAR TEXTO ---
text_cols = ["Descripcion", "Direccion", "Localidad", "Provincia", "TipoTienda"]
for col in text_cols:
    df[col] = df[col].astype(str).str.title().replace('Nan', None)

# --- VALIDAR ID ---
df["CodTienda"] = pd.to_numeric(df["CodTienda"], errors='coerce').fillna(0).astype(int)

# Eliminar duplicados
df.drop_duplicates(subset="CodTienda", keep="first", inplace=True)

print(f"TRANSFORM OK – Registros limpios: {len(df)}")

# ============================================================
# 4. LOAD
# ============================================================
try:
    # Usamos el nombre exacto de tu tabla: STG_Dim_Tienda
    df.to_sql("STG_Dim_Tienda", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en STG_Dim_Tienda")
except Exception as e:
    print(f"❌ ERROR al cargar en STAGING: {e}")

# Guardar CSV limpio para QA
df.to_csv(os.path.join(QA_PATH, "Tiendas_limpio.csv"), index=False, encoding='utf-8-sig')

print("====================================")