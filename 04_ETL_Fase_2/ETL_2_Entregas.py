import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================")
print("ETL 2 – STG A INT (ENTREGAS)")
print("====================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS Y CONEXIÓN
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"

# Origen: Archivos que vienen de la etapa de Staging (STG)
ORIG_PATH = os.path.join(BASE_PATH, "03_STG", "Archivos_Limpios")

# Destino: Archivos procesados para la capa de Integración (INT)
DEST_PATH = os.path.join(BASE_PATH, "05_INT", "Archivos_Limpios")
os.makedirs(DEST_PATH, exist_ok=True)

# Configuración de SQL Server
server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
db_name = 'DW_DataShop'
conn_str = f"mssql+pyodbc://{server_name}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
engine = create_engine(conn_str)

# ============================================================
# 2. LECTURA DEL ARCHIVO DE STAGING
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Entregas_limpio.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error crítico: No se encontró el archivo {csv_path}")
    print("Asegúrate de que el archivo generado se haya copiado a STG como 'Entregas_limpio.csv'")
    exit()

df = pd.read_csv(csv_path, encoding="utf-8-sig")

# ============================================================
# 3. TRANSFORMACIÓN Y LIMPIEZA (REGLAS DE NEGOCIO)
# ============================================================

# --- RN 1: Tratamiento de Fechas ---
# Usamos un bucle para evitar errores si la columna no existe aún
cols_fechas = ["Fecha_Envio", "Fecha_Entrega_Estimada", "Fecha_Entrega_Real"]
for col in cols_fechas:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

# --- RN 2: Forzar IDs a Enteros (Claves Foráneas) ---
cols_ids = ["CodEntrega", "CodVenta", "CodProveedor", "CodRango", "CodAlmacen", "CodEstado"]
for col in cols_ids:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

# --- RN 3: Tratamiento de Métricas Numéricas ---
cols_metricas = ["Costo_Total_Entrega", "Costo_Por_Km", "Distancia_Recorrida_Km"]
for col in cols_metricas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

# --- RN 4: Estandarización de Estructura (Manejo de NULLs) ---
# Definimos el orden exacto de la tabla SQL INT_Fact_Entregas
columnas_finales = [
    "CodEntrega", "CodVenta", "Fecha_Envio", "Fecha_Entrega_Estimada", 
    "Fecha_Entrega_Real", "Tiempo_Entrega", "Costo_Total_Entrega", 
    "Entregado_A_Tiempo", "Costo_Por_Km", "Distancia_Recorrida_Km", 
    "CodProveedor", "CodRango", "CodAlmacen", "CodEstado"
]

# reindex crea las columnas faltantes con valores NULL (NaN)
df_int = df.reindex(columns=columnas_finales)

# Rellenamos valores de texto para pedidos pendientes
df_int["Tiempo_Entrega"] = df_int["Tiempo_Entrega"].fillna("Pendiente")
df_int["Entregado_A_Tiempo"] = df_int["Entregado_A_Tiempo"].fillna("Pendiente")

print(f"✔ Transformación completada: {len(df_int)} registros procesados.")

# ============================================================
# 4. CARGA A SQL SERVER (CAPA INT)
# ============================================================

try:
    # 'append' permite que SQL Server asigne automáticamente la fecha de carga si existe el DEFAULT
    df_int.to_sql("INT_Fact_Entregas", engine, if_exists="append", index=False)
    print("✔ LOAD OK – Datos inyectados exitosamente en la tabla INT_Fact_Entregas")
except Exception as e:
    print(f"❌ Error durante la carga a SQL Server: {e}")

# ============================================================
# 5. EXPORTACIÓN DE RESPALDO
# ============================================================
final_csv = os.path.join(DEST_PATH, "Entregas_INT.csv")
df_int.to_csv(final_csv, index=False, encoding="utf-8-sig")

print(f"✔ Respaldo guardado en: {final_csv}")
print("====================================")