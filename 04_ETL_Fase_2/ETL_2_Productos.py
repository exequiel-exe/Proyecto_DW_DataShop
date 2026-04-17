import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (PRODUCTOS)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS 
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Origen: Carpeta 03_STG
ORIG_PATH = os.path.join(BASE_PATH, "03_STG", "Archivos_Limpios")
# Destino: Carpeta 05_INT
DEST_PATH = os.path.join(BASE_PATH, "05_INT", "Archivos_Limpios")
os.makedirs(DEST_PATH, exist_ok=True)

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# ============================================================
# 2. LECTURA
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Productos_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ No se encontró el archivo: {csv_path}")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN (INT)
# ============================================================

# --- RN 1: Asegurar consistencia de tipos para INT ---
# Como tu tabla INT pide VARCHAR para precios, nos aseguramos de que sean strings
df["PrecioCosto"] = df["PrecioCosto"].astype(str)
df["PrecioVentaSugerido"] = df["PrecioVentaSugerido"].astype(str)

# --- RN 2: Manejo de Nulos en Categoría/Marca ---
df["Categoria"] = df["Categoria"].fillna("SIN CATEGORIA")
df["Marca"] = df["Marca"].fillna("SIN MARCA")

# --- RN 3: Preparación de Columnas Finales ---
# Definimos el orden exacto de tu CREATE TABLE (excluimos FechaCarga por el DEFAULT de SQL)
columnas_finales = [
    "CodProducto", "Descripcion", "Categoria", 
    "Marca", "PrecioCosto", "PrecioVentaSugerido"
]

# Filtramos solo las columnas necesarias
df_int = df[columnas_finales].copy()

print(f"✔ Transformación completada: {len(df_int)} productos listos.")

# ============================================================
# 4. CARGA A SQL (Tabla INT_Dim_Producto)
# ============================================================
try:
    # Usamos 'append' para respetar el DEFAULT de FechaCarga en SQL
    df_int.to_sql("INT_Dim_Producto", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en INT_Dim_Producto")
except Exception as e:
    print(f"❌ ERROR de carga en INT: {e}")

# ============================================================
# 5. EXPORTACIÓN
# ============================================================
final_path = os.path.join(DEST_PATH, "Productos_INT.csv")
df_int.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"✔ Archivo INT guardado en: {final_path}")
print("====================================")