import os
import pandas as pd
from sqlalchemy import create_engine
import logging

print("====================================")
print("ETL 1 – CLIENTES (STG)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
ORIG_PATH = os.path.join(BASE_PATH, "01_RAW", "Datos_Originales")
csv_path = os.path.join(ORIG_PATH, "Clientes.csv")

STG_PATH = os.path.join(BASE_PATH, "02_STG")
QA_PATH = os.path.join(STG_PATH, "Archivos_Limpios")
os.makedirs(QA_PATH, exist_ok=True)

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# ============================================================
# 2. EXTRACT
# ============================================================
try:
    # CLAVE: Forzamos Telefono y CP a string desde la lectura
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={"Telefono": str, "CP": str})
    print(f"Archivo leído. Registros: {len(df)}")
except Exception as e:
    print(f"ERROR al leer: {e}")
    exit()

# ============================================================
# 3. TRANSFORM
# ============================================================
df.columns = ["CodCliente", "RazonSocial", "Telefono", "Email", "Direccion", "Localidad", "Provincia", "CP"]

# --- LIMPIEZA DE TELÉFONO (Evita el .0 y caracteres raros) ---
def limpiar_telefono(tel):
    if pd.isna(tel) or str(tel).lower() in ['nan', 'none', '']:
        return None
    t = str(tel).strip()
    if t.endswith('.0'): t = t[:-2] # Elimina el .0 decimal si existe
    return t

df["Telefono"] = df["Telefono"].apply(limpiar_telefono)

# --- LIMPIEZA DE CP ---
df["CP"] = df["CP"].apply(lambda x: str(x).split('.')[0].zfill(4) if pd.notna(x) and x != '' else None)

# --- RESTO DE TRANSFORMACIONES ---
df.dropna(how="all", inplace=True)
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.replace({"": None, " ": None}, inplace=True)

text_cols = ["RazonSocial", "Direccion", "Localidad", "Provincia"]
for col in text_cols:
    df[col] = df[col].astype(str).str.title().replace('Nan', None)

df["Email"] = df["Email"].apply(lambda x: x if isinstance(x, str) and "@" in x else None)
df["CodCliente"] = pd.to_numeric(df["CodCliente"], errors='coerce').fillna(0).astype(int)
df.drop_duplicates(subset="CodCliente", keep="first", inplace=True)

print(f"Registros limpios: {len(df)}")

# ============================================================
# 4. LOAD
# ============================================================
try:
    df.to_sql("STG_Dim_Clientes", engine, if_exists="append", index=False)
    print("✔ LOAD OK en STG_Dim_Clientes")
except Exception as e:
    print(f"❌ ERROR de carga: {e}")

# Guardar para QA
df.to_csv(os.path.join(QA_PATH, "Clientes_limpio.csv"), index=False, encoding='utf-8-sig')
print("====================================")