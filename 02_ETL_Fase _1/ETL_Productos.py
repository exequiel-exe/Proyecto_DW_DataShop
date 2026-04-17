import os
import pandas as pd
from sqlalchemy import create_engine
import logging

print("====================================")
print("ETL 1 – PRODUCTOS (STG)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Ajuste a la nueva ruta RAW
ORIG_PATH = os.path.join(BASE_PATH, "01_RAW", "Datos_Originales")
csv_path = os.path.join(ORIG_PATH, "Productos.csv")

# Rutas para logs y QA en la carpeta STG
STG_PATH = os.path.join(BASE_PATH, "02_STG")
QA_PATH = os.path.join(STG_PATH, "Archivos_Limpios")
os.makedirs(QA_PATH, exist_ok=True)

logging.basicConfig(filename=os.path.join(STG_PATH, 'etl_productos_errors.log'), 
                    level=logging.WARNING, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

print("Leyendo archivo:", csv_path)

# ============================================================
# 2. EXTRACT
# ============================================================

try:
    # Leemos forzando el código como string para evitar el ".0"
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={"CodProducto": str})
    print(f"Archivo leído correctamente. Registros: {len(df)}")
except Exception as e:
    print(f"ERROR al leer: {e}")
    exit()

# ============================================================
# 3. TRANSFORM
# ============================================================

# Forzamos los nombres de columnas de tu CREATE TABLE
df.columns = ["CodProducto", "Descripcion", "Categoria", "Marca", "PrecioCosto", "PrecioVentaSugerido"]

# Eliminar filas completamente vacías
df.dropna(how="all", inplace=True)

# Remover espacios en blanco
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Reemplazar valores vacíos por NULL
df.replace({"": None, " ": None}, inplace=True)

# Estandarizar texto a Capitalización Normal
text_cols = ["Descripcion", "Categoria", "Marca"]
for col in text_cols:
    df[col] = df[col].astype(str).str.title().replace('Nan', None)

# Validar CodProducto como int limpio (sin .0)
df["CodProducto"] = pd.to_numeric(df["CodProducto"], errors='coerce').fillna(0).astype(int)

# Convertir precios a numérico (se mapeará a DECIMAL en SQL)
df["PrecioCosto"] = pd.to_numeric(df["PrecioCosto"], errors='coerce').round(2)
df["PrecioVentaSugerido"] = pd.to_numeric(df["PrecioVentaSugerido"], errors='coerce').round(2)

# Eliminar duplicados por clave de producto
df.drop_duplicates(subset="CodProducto", keep="first", inplace=True)

print(f"TRANSFORM OK – Registros tras limpieza: {len(df)}")

# ============================================================
# 4. LOAD
# ============================================================

try:
    # IMPORTANTE: Usamos "STG_Dim_Productos" y "append"
    df.to_sql("STG_Dim_Productos", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en STG_Dim_Productos")
except Exception as e:
    print("❌ ERROR al cargar datos en STAGING:", e)

# Guardar CSV limpio para QA
csv_limpio_path = os.path.join(QA_PATH, "Productos_limpio.csv")
df.to_csv(csv_limpio_path, index=False, encoding='utf-8-sig')

print("====================================")
print("ETL FINALIZADO CORRECTAMENTE")
print("====================================")