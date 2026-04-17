import os
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

print("====================================================")
print("   ETL 3 – CALIDAD PRO: DIM_PROVEEDORES (FINAL)    ")
print("====================================================")

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS Y CONEXIÓN
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
ORIG_PATH = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados")
DEST_PATH = os.path.join(BASE_PATH, "07_DW", "Datos_Finales")
os.makedirs(DEST_PATH, exist_ok=True)

server_name = 'DESKTOP-TPLKQNF\\SQLSERVEREXP2019'
engine = create_engine(f"mssql+pyodbc://{server_name}/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# ============================================================
# 2. EXTRACCIÓN
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Proveedores_generado.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se localizó el archivo en {csv_path}")
    exit()

df_raw = pd.read_csv(csv_path, encoding="utf-8-sig")
total_inicial = len(df_raw)

# ============================================================
# 3. TRANSFORMACIÓN Y DATA QUALITY
# ============================================================

# --- A. Gestión de Duplicados ---
df_clean = df_raw.drop_duplicates(subset=['CodProveedor'], keep='first').copy()

# --- B. Limpieza de Espacios y Normalización ---
for col in ['Proveedor']:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].astype(str).str.strip().str.title()

# --- C. Limpieza de Valores Numéricos ---
for col in ['Costo_Base', 'Tarifa_Km']:
    if col in df_clean.columns:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0.0)

# ============================================================
# 4. MATCHING SK (TRANSFORMACIÓN CLAVE)
# ============================================================
# Traemos la tabla de rangos de la DB para obtener las SK reales
try:
    with engine.connect() as conn:
        df_rangos = pd.read_sql("SELECT SK_RangoDistancia, CodRango FROM Dim_RangoDistancia", conn)
    
    # Cruzamos el DataFrame de proveedores con el de rangos por 'CodRango'
    df_merged = pd.merge(df_clean, df_rangos, on='CodRango', how='inner')
    
    inconsistencias_rango = len(df_clean) - len(df_merged)
except Exception as e:
    print(f"❌ Error al obtener SK de Rangos: {e}")
    exit()

# ============================================================
# 5. MAPEO A ESTRUCTURA DIMENSIÓN
# ============================================================

# Renombramos 'Proveedor' a 'Nombre_Proveedor' según tu SQL
df_merged = df_merged.rename(columns={'Proveedor': 'Nombre_Proveedor'})

columnas_dim = [
    "CodProveedor", "Nombre_Proveedor", "Costo_Base", 
    "Km_Incluidos", "Tarifa_Km", "SK_RangoDistancia"
]

df_dim = df_merged.reindex(columns=columnas_dim)

# ============================================================
# 6. CARGA A SQL SERVER (DIM_PROVEEDORES)
# ============================================================

try:
    # SQL genera el ProveedorSK automáticamente
    # Limpiamos antes para evitar duplicados si corres el script de nuevo
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE Dim_Proveedores"))
        conn.commit()
    
    df_dim.to_sql("Dim_Proveedores", engine, if_exists="append", index=False)
    
    print("\n✅ --- REPORTE DE CALIDAD: PROVEEDORES ---")
    print(f"| Registros Originales: {total_inicial}")
    print(f"| Proveedores Cargados: {len(df_dim)}")
    print(f"| Match SK_RangoDistancia: Exitoso")
    print(f"| Proveedores descartados por Rango inexistente: {inconsistencias_rango}")
    print("----------------------------------------\n")
    
except Exception as e:
    print(f"❌ Error crítico en la carga: {e}")

# ============================================================
# 7. EXPORTACIÓN FINAL (CAPA 07)
# ============================================================
final_path = os.path.join(DEST_PATH, "Proveedores_DW_Final.csv")
df_dim.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"💾 Respaldo final guardado en: {final_path}")
print("====================================================")