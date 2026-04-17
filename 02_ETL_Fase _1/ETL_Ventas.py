import os
import pandas as pd
from sqlalchemy import create_engine
import logging

print("====================================")
print("ETL 1 – VENTAS (STG)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Nueva ruta de archivos originales
ORIG_PATH = os.path.join(BASE_PATH, "01_RAW", "Datos_Originales")
csv_path = os.path.join(ORIG_PATH, "Ventas.csv")

# Rutas para logs y QA en la carpeta STG
STG_PATH = os.path.join(BASE_PATH, "02_STG")
QA_PATH = os.path.join(STG_PATH, "Archivos_Limpios")
os.makedirs(QA_PATH, exist_ok=True)

logging.basicConfig(filename=os.path.join(STG_PATH, 'etl_ventas_errors.log'), 
                    level=logging.WARNING, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

print("Leyendo archivo:", csv_path)

# ============================================================
# 2. EXTRACT
# ============================================================

try:
    # Leemos forzando los códigos como string para evitar el ".0"
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={
        "CodProducto": str, 
        "CodCliente": str, 
        "CodTienda": str
    })
    print(f"Archivo leído correctamente. Registros: {len(df)}")
except Exception as e:
    print(f"ERROR al leer el archivo: {e}")
    exit()

# ============================================================
# 3. TRANSFORM
# ============================================================

# Forzamos los nombres de columnas de tu CREATE TABLE
df.columns = ["FechaVenta", "CodProducto", "Producto", "Cantidad", "PrecioVenta", "CodCliente", "Cliente", "CodTienda", "Tienda"]

# Eliminar filas completamente vacías
df.dropna(how="all", inplace=True)

# Remover espacios en blanco
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Reemplazar valores vacíos por NULL
df.replace({"": None, " ": None}, inplace=True)

# --- TRATAMIENTO DE FECHAS ---
# Las convertimos a formato datetime de Python para SQL
df["FechaVenta"] = pd.to_datetime(df["FechaVenta"], errors='coerce')

# --- NORMALIZAR TEXTOS ---
text_cols = ["Producto", "Cliente", "Tienda"]
for col in text_cols:
    df[col] = df[col].astype(str).str.title().replace('Nan', None)

# --- VALIDAR CÓDIGOS E IDS (Sin decimales) ---
ids_cols = ["CodProducto", "CodCliente", "CodTienda"]
for col in ids_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

# --- CANTIDAD Y PRECIOS ---
df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors='coerce').fillna(0).astype(int)
df["PrecioVenta"] = pd.to_numeric(df["PrecioVenta"], errors='coerce').round(2)

# Log de errores para revisión
errores = df[df.isnull().any(axis=1)]
if not errores.empty:
    logging.warning(f"Filas con nulos encontrados: {len(errores)}")

print(f"TRANSFORM OK – Registros limpios: {len(df)}")

# ============================================================
# 4. LOAD
# ============================================================

try:
    # IMPORTANTE: Nombre de tabla "STG_Fact_Ventas" y modo "append"
    df.to_sql("STG_Fact_Ventas", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en STG_Fact_Ventas")
except Exception as e:
    print(f"❌ ERROR al cargar en STAGING: {e}")

# Guardar CSV limpio para QA
df.to_csv(os.path.join(QA_PATH, "Ventas_limpio.csv"), index=False, encoding='utf-8-sig')

print("====================================")
print("ETL FINALIZADO CORRECTAMENTE")
print("====================================")