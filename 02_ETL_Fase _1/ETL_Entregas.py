import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 1 – ENTREGAS (STG)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
ORIG_PATH = os.path.join(BASE_PATH, "01_RAW", "Datos_Originales")
STG_PATH = os.path.join(BASE_PATH, "02_STG")
QA_PATH = os.path.join(STG_PATH, "Archivos_Limpios")
os.makedirs(QA_PATH, exist_ok=True)

server_name = r"DESKTOP-TPLKQNF\SQLSERVEREXP2019"
engine = create_engine(
    f"mssql+pyodbc://{server_name}/DW_DataShop?"
    "driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
)

# ============================================================
# 2. EXTRACT
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Entregas.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se encontró el archivo en {csv_path}")
    exit()

# Leemos forzando los IDs como strings para evitar el ".0"
df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={
    "CodEntrega": str, 
    "CodVenta": str, 
    "CodProveedor": str, 
    "CodAlmacen": str, 
    "CodEstado": str
})
print("EXTRACT OK – Registros originales:", len(df))

# ============================================================
# 3. TRANSFORM
# ============================================================

# Eliminar filas completamente vacías
df.dropna(how="all", inplace=True)

# Limpiar espacios
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# --- TRATAMIENTO DE CLAVES NUMÉRICAS ---
num_cols = ["CodEntrega", "CodVenta", "CodProveedor", "CodAlmacen", "CodEstado"]
for col in num_cols:
    # Convertimos a numérico, los errores se vuelven NaN y luego a 0 para el INT de SQL
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

# CodEntrega es obligatorio (si es 0, lo eliminamos)
df = df[df["CodEntrega"] != 0]

# --- TRATAMIENTO DE FECHAS ---
# Las convertimos a formato datetime de Python para que SQLAlchemy las mapee a DATE de SQL
df["Fecha_Envio"] = pd.to_datetime(df["Fecha_Envio"], errors="coerce")
df["Fecha_Entrega"] = pd.to_datetime(df["Fecha_Entrega"], errors="coerce")

# --- NORMALIZAR TEXTOS ---
text_cols = ["Proveedor", "Almacen", "Estado"]
for col in text_cols:
    df[col] = df[col].astype(str).str.title().replace("Nan", None)

# Eliminar duplicados por clave de entrega
df.drop_duplicates(subset="CodEntrega", inplace=True)

print(f"TRANSFORM OK – Registros limpios para cargar: {len(df)}")

# ============================================================
# 4. LOAD – STAGING
# ============================================================
try:
    # IMPORTANTE: "STG_Fact_Entregas" y "append"
    df.to_sql(
        "STG_Fact_Entregas", 
        engine, 
        if_exists="append", 
        index=False
    )
    print("✔ LOAD OK – Datos inyectados en STG_Fact_Entregas")
except Exception as e:
    print(f"❌ ERROR al cargar STG: {e}")

# ============================================================
# 5. EXPORT CSV LIMPIO (QA)
# ============================================================
csv_limpio_path = os.path.join(QA_PATH, "Entregas_limpio.csv")
df.to_csv(csv_limpio_path, index=False, encoding="utf-8-sig")

print("QA OK – Archivo guardado para revisión.")
print("====================================")