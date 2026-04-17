import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 1 – ESTADO DEL PEDIDO (STG)")
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
csv_path = os.path.join(ORIG_PATH, "EstadoDelPedido.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se encontró el archivo en {csv_path}")
    exit()

# Forzamos CodEstado como string para evitar el .0 inicial
df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={"CodEstado": str})
print("EXTRACT OK – Registros originales:", len(df))

# ============================================================
# 3. TRANSFORM
# ============================================================

# Eliminar nulos y duplicados generales
df = df.dropna(how="all")
df = df.drop_duplicates()

# Limpiar espacios
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Asegurar que CodEstado sea INT limpio (sin decimales)
df["CodEstado"] = pd.to_numeric(df["CodEstado"], errors="coerce")
df = df[df["CodEstado"].notna()]
df["CodEstado"] = df["CodEstado"].astype(int)

# Normalizar texto de descripción
df["Descripcion_Estado"] = df["Descripcion_Estado"].astype(str).str.title()

# Eliminar duplicados por clave (por si acaso)
df.drop_duplicates(subset="CodEstado", inplace=True)

print(f"TRANSFORM OK – Registros limpios: {len(df)}")

# ============================================================
# 4. LOAD – STAGING
# ============================================================
try:
    # Usamos "STG_Dim_EstadoPedido" y "append" para respetar tu SQL
    df.to_sql(
        "STG_Dim_EstadoPedido", 
        engine, 
        if_exists="append", 
        index=False
    )
    print("✔ LOAD OK – Datos inyectados en STG_Dim_EstadoPedido")
except Exception as e:
    print(f"❌ ERROR al cargar STG: {e}")

# ============================================================
# 5. EXPORT CSV LIMPIO (QA)
# ============================================================
csv_limpio_path = os.path.join(QA_PATH, "EstadoDelPedido_limpio.csv")
df.to_csv(csv_limpio_path, index=False, encoding="utf-8-sig")

print("QA OK – Archivo de control generado.")
print("====================================")