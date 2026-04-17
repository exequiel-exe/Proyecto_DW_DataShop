import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (TIENDAS)")
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
csv_path = os.path.join(ORIG_PATH, "Tiendas_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ No se encontró el archivo: {csv_path}")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN (INT)
# ============================================================

# --- RN 1: Asegurar que CodTienda y CP sean tratados como Texto ---
df["CodTienda"] = df["CodTienda"].astype(str)
df["CP"] = df["CP"].astype(str)

# --- RN 2: Rellenar nulos en campos descriptivos ---
df["TipoTienda"] = df["TipoTienda"].fillna("No Definido")
df["Localidad"] = df["Localidad"].fillna("Desconocida")

# --- RN 3: Mapeo de columnas para INT ---
# Coincidimos con el CREATE TABLE (CodTienda, Descripcion, Direccion, Localidad, Provincia, CP, TipoTienda)
columnas_finales = [
    "CodTienda", "Descripcion", "Direccion", 
    "Localidad", "Provincia", "CP", "TipoTienda"
]

# Filtramos y reordenamos
df_int = df[columnas_finales].copy()

print(f"✔ Datos de Tiendas listos: {len(df_int)} registros.")

# ============================================================
# 4. CARGA A SQL (Tabla INT_Dim_Tienda)
# ============================================================
try:
    # Usamos 'append' para que SQL Server maneje el DEFAULT de FechaCarga
    df_int.to_sql("INT_Dim_Tienda", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en INT_Dim_Tienda")
except Exception as e:
    print(f"❌ ERROR de carga en INT: {e}")

# ============================================================
# 5. EXPORTACIÓN
# ============================================================
final_path = os.path.join(DEST_PATH, "Tiendas_INT.csv")
df_int.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"✔ Archivo INT guardado en: {final_path}")
print("====================================")