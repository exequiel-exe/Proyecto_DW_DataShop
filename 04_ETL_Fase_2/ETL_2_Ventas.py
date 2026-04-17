import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (VENTAS)")
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
csv_path = os.path.join(ORIG_PATH, "Ventas_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ No se encontró el archivo: {csv_path}")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN (INT)
# ============================================================

# --- RN 1: Tratamiento de Fechas ---
df["FechaVenta"] = pd.to_datetime(df["FechaVenta"], errors='coerce')

# --- RN 2: Forzar Códigos a VARCHAR (como pide la tabla INT) ---
df["CodProducto"] = df["CodProducto"].astype(str)
df["CodCliente"] = df["CodCliente"].astype(str)
df["CodTienda"] = df["CodTienda"].astype(str)

# --- RN 3: Tratamiento Numérico (Precios y Cantidades) ---
df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors='coerce').fillna(0).astype(int)
df["PrecioVenta"] = pd.to_numeric(df["PrecioVenta"], errors='coerce').fillna(0.0)

# --- RN 4: Limpieza de Textos ---
df["Producto"] = df["Producto"].fillna("Sin Descripción")
df["Cliente"] = df["Cliente"].fillna("Consumidor Final")
df["Tienda"] = df["Tienda"].fillna("Tienda General")

# Definimos el orden exacto de tu CREATE TABLE INT_Fact_Ventas
columnas_finales = [
    "FechaVenta", "CodProducto", "Producto", "Cantidad", 
    "PrecioVenta", "CodCliente", "Cliente", "CodTienda", "Tienda"
]

df_int = df[columnas_finales].copy()

print(f"✔ Datos de Ventas procesados: {len(df_int)} registros.")

# ============================================================
# 4. CARGA A SQL (Tabla INT_Fact_Ventas)
# ============================================================
try:
    # Usamos append para que SQL maneje el FechaCarga DEFAULT
    df_int.to_sql("INT_Fact_Ventas", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en INT_Fact_Ventas")
except Exception as e:
    print(f"❌ ERROR de carga en INT: {e}")

# ============================================================
# 5. EXPORTACIÓN
# ============================================================
final_path = os.path.join(DEST_PATH, "Ventas_INT.csv")
df_int.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"✔ Archivo INT guardado en: {final_path}")
print("====================================")