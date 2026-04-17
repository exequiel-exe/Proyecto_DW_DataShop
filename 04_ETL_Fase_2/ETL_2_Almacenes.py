import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (ALMACENES)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
ORIG_PATH = os.path.join(BASE_PATH, "03_STG", "Archivos_Limpios")
DEST_PATH = os.path.join(BASE_PATH, "05_INT", "Archivos_Limpios")
os.makedirs(DEST_PATH, exist_ok=True)

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# ============================================================
# 2. LECTURA
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Almacenes_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ No se encontró el archivo: {csv_path}")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN (INT)
# ============================================================

# --- RN 1: Mapeo de columnas ---
# En STG teníamos 'Ubicacion'. Lo mapeamos a 'Localidad' según tu tabla INT.
if "Ubicacion" in df.columns:
    df.rename(columns={"Ubicacion": "Localidad"}, inplace=True)

# --- RN 2: Columnas Nuevas ---
# Tu tabla INT pide 'Provincia', la creamos con valor nulo si no existe en el origen
if "Provincia" not in df.columns:
    df["Provincia"] = None

# --- RN 3: Limpieza y tipos ---
df["CodAlmacen"] = pd.to_numeric(df["CodAlmacen"], errors="coerce")

# Definimos el orden exacto de tu CREATE TABLE INT_Dim_Almacenes
columnas_finales = ["CodAlmacen", "Nombre_Almacen", "Localidad", "Provincia"]

df_int = df[columnas_finales].copy()

print(f"✔ Datos de Almacenes listos: {len(df_int)} registros.")

# ============================================================
# 4. CARGA A SQL (Tabla INT_Dim_Almacenes)
# ============================================================
try:
    # Usamos 'append' para respetar la estructura
    df_int.to_sql("INT_Dim_Almacenes", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en INT_Dim_Almacenes")
except Exception as e:
    print(f"❌ ERROR de carga en INT: {e}")

# ============================================================
# 5. EXPORTACIÓN
# ============================================================
final_path = os.path.join(DEST_PATH, "Almacenes_INT.csv")
df_int.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"✔ Archivo INT guardado en: {final_path}")
print("====================================")