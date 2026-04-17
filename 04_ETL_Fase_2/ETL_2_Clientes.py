import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (CLIENTES)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Origen: Archivo generado por ETL 1
ORIG_PATH = os.path.join(BASE_PATH, "03_STG", "Archivos_Limpios")
# Destino: Carpeta de la fase intermedia
DEST_PATH = os.path.join(BASE_PATH, "05_INT", "Archivos_Limpios")
os.makedirs(DEST_PATH, exist_ok=True)

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# ============================================================
# 2. LECTURA (Desde STG QA)
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Clientes_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ No se encontró el archivo: {csv_path}")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN Y REGLAS DE NEGOCIO (INT)
# ============================================================

# --- RN 1: Mapeo de Columnas Existentes ---
# En el CSV se llama 'RazonSocial', pero en INT se llama 'NombreCompleto'
df.rename(columns={'RazonSocial': 'NombreCompleto'}, inplace=True)

# --- RN 2: Manejo de Nulos Críticos ---
df["Email"] = df["Email"].fillna("SIN_DATO")
df["Telefono"] = df["Telefono"].fillna("000-0000")

# --- RN 3: Agregar Columnas Nuevas de la tabla INT ---
# Estas columnas no existen en el CSV, las creamos para coincidir con el CREATE TABLE
df["Sexo"] = None  # Se cargará como NULL en SQL
df["Edad"] = None  # Se cargará como NULL en SQL

# Nota: FechaCarga no la agregamos aquí porque el SQL tiene un DEFAULT (GETDATE())
# Pero si prefieres enviarla desde Python, descomenta la siguiente línea:
# df["FechaCarga"] = pd.Timestamp.now()

# Seleccionamos y ordenamos las columnas para que coincidan EXACTO con el SQL de INT
# Esto evita errores de "Invalid column name"
columnas_finales = [
    "CodCliente", "NombreCompleto", "Telefono", "Email", 
    "Direccion", "Localidad", "Provincia", "CP", "Sexo", "Edad"
]

# Si falta alguna columna por error, la creamos vacía antes de filtrar
for col in columnas_finales:
    if col not in df.columns:
        df[col] = None

df_int = df[columnas_finales].copy()

print(f"✔ Datos preparados: {len(df_int)} registros.")

# ============================================================
# 4. CARGA A SQL (Tabla INT)
# ============================================================
try:
    # Usamos 'append' para no borrar la estructura que ya creaste
    df_int.to_sql("INT_Dim_Cliente", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en INT_Dim_Cliente")
except Exception as e:
    print(f"❌ ERROR de carga en INT: {e}")

# ============================================================
# 5. EXPORTACIÓN (Archivo Final INT)
# ============================================================
final_path = os.path.join(DEST_PATH, "Clientes_INT.csv")
df_int.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"✔ Archivo INT guardado en: {final_path}")
print("====================================")