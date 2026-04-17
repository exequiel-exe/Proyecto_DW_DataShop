import os
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

print("====================================================")
print("   ETL 3 – CALIDAD PRO: FACT_ENTREGAS (FINAL)      ")
print("====================================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS Y CONEXIÓN
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
ORIG_PATH = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados")
DEST_PATH = os.path.join(BASE_PATH, "07_DW", "Datos_Finales")

# Crear carpeta de destino si no existe
os.makedirs(DEST_PATH, exist_ok=True)

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(
    f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes",
    fast_executemany=True
)

# ============================================================
# 2. EXTRACCIÓN
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Entregas_generado.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se localizó el archivo en {csv_path}")
    exit()

df_raw = pd.read_csv(csv_path, encoding="utf-8-sig")
total_inicial = len(df_raw)

# ============================================================
# 3. TRANSFORMACIÓN Y DATA QUALITY
# ============================================================

# --- A. Limpieza de Espacios e Identificación de Nulos ---
for col in df_raw.columns:
    if df_raw[col].dtype == 'object':
        df_raw[col] = df_raw[col].astype(str).str.strip()

# --- B. Manejo de Nulos Críticos (Evita error 'nan' en SQL) ---
df_raw['Fecha_Entrega_Real'] = df_raw['Fecha_Entrega_Real'].replace(['nan', 'None', 'NaN', ''], None)

# --- C. Conversión de Métrica de Tiempo ---
df_raw['Tiempo_Entrega_Dias'] = df_raw['Tiempo_Entrega'].str.extract(r'(\d+)').fillna(0).astype(int)

# ============================================================
# 4. MATCHING SK (CONSOLIDACIÓN DE DIMENSIONES)
# ============================================================
try:
    with engine.connect() as conn:
        print("🔍 Consultando SKs de las dimensiones en el DW...")
        
        dim_ventas = pd.read_sql("SELECT VentaSK FROM FACT_Ventas", conn)
        dim_ventas['CodVenta'] = dim_ventas['VentaSK']
        
        dim_prov = pd.read_sql("SELECT SK_Proveedor, CodProveedor FROM DIM_Proveedores", conn)
        dim_rango = pd.read_sql("SELECT SK_RangoDistancia, CodRango FROM DIM_RangoDistancia", conn)
        dim_estado = pd.read_sql("SELECT SK_EstadoPedido, CodEstado FROM DIM_EstadoPedido", conn)
        
        # --- NUEVA COLUMNA: ALMACÉN ---
        dim_almacen = pd.read_sql("SELECT SK_Almacen, CodAlmacen FROM DIM_Almacen", conn)

    # Cruces (Joins en memoria)
    df_merged = df_raw.merge(dim_ventas, on='CodVenta', how='inner')
    df_merged = df_merged.merge(dim_prov, on='CodProveedor', how='inner')
    df_merged = df_merged.merge(dim_rango, on='CodRango', how='inner')
    df_merged = df_merged.merge(dim_estado, on='CodEstado', how='inner')
    df_merged = df_merged.merge(dim_almacen, on='CodAlmacen', how='inner') # <--- NUEVO MERGE

    inconsistencias = total_inicial - len(df_merged)

except Exception as e:
    print(f"❌ Error al obtener SKs de la DB: {e}")
    exit()

# ============================================================
# 5. MAPEO A ESTRUCTURA FINAL (SEGÚN TU SQL)
# ============================================================
df_final = df_merged.rename(columns={
    'Distancia_Recorrida_Km': 'Distancia_Km',
    'Costo_Total_Entrega': 'Costo_Total'
})

columnas_fact = [
    'VentaSK', 'SK_Proveedor', 'SK_RangoDistancia', 'SK_EstadoPedido', 'SK_Almacen', # <--- AGREGADA SK_ALMACEN
    'Fecha_Envio', 'Fecha_Estimada', 'Fecha_Entrega_Real',
    'Distancia_Km', 'Costo_Total', 'Costo_Por_Km',
    'Tiempo_Entrega_Dias', 'Entregado_A_Tiempo'
]

df_fact = df_final.reindex(columns=columnas_fact)

# ============================================================
# 6. CARGA A SQL SERVER (FACT_ENTREGAS)
# ============================================================
try:
    with engine.begin() as conn:
        print("🚀 Limpiando tabla FACT_Entregas e iniciando carga masiva...")
        conn.execute(text("TRUNCATE TABLE FACT_Entregas"))
        
        df_fact.to_sql("FACT_Entregas", conn, if_exists="append", index=False, chunksize=1000)
    
    print("\n✅ --- REPORTE DE CALIDAD FINAL ---")
    print(f"| Registros procesados: {total_inicial}")
    print(f"| Registros insertados: {len(df_fact)}")
    print(f"| Registros perdidos por inconsistencia: {inconsistencias}")
    print("----------------------------------------\n")
    
except Exception as e:
    print(f"\n❌ ERROR CRÍTICO EN CARGA SQL: {e}")

# ============================================================
# 7. EXPORTACIÓN DE RESPALDO (CAPA 07)
# ============================================================
final_path = os.path.join(DEST_PATH, "Fact_Entregas_DW_Final.csv")
df_fact.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"💾 Respaldo CSV generado en: {final_path}")
print("====================================================")