import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (ESTADO PEDIDO)")
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
csv_path = os.path.join(ORIG_PATH, "EstadoDelPedido_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ No se encontró el archivo: {csv_path}")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN (PASSTHROUGH)
# ============================================================

# Nos aseguramos de que las columnas se llamen como en la tabla INT
# SQL: CodEstado, Descripcion_Estado
columnas_finales = ["CodEstado", "Descripcion_Estado"]

# Si por alguna razón los nombres en STG fueran diferentes, los forzamos aquí
df.columns = columnas_finales 

df_int = df[columnas_finales].copy()

print(f"✔ Datos de Estados listos: {len(df_int)} registros.")

# ============================================================
# 4. CARGA A SQL (Tabla INT_Dim_EstadoPedido)
# ============================================================
try:
    df_int.to_sql("INT_Dim_EstadoPedido", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados en INT_Dim_EstadoPedido")
except Exception as e:
    print(f"❌ ERROR de carga en INT: {e}")

# ============================================================
# 5. EXPORTACIÓN
# ============================================================
final_path = os.path.join(DEST_PATH, "EstadoPedido_INT.csv")
df_int.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"✔ Archivo INT guardado en: {final_path}")
print("====================================")