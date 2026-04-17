import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

print("====================================================")
print("   ETL 3 – CALIDAD PRO: DIM_PRODUCTO (FINAL)       ")
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
csv_path = os.path.join(ORIG_PATH, "Productos_generado.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se localizó el archivo en {csv_path}")
    exit()

df_raw = pd.read_csv(csv_path, encoding="utf-8-sig")
total_inicial = len(df_raw)

# ============================================================
# 3. TRANSFORMACIÓN Y LIMPIEZA PROFUNDA (DATA QUALITY)
# ============================================================

# --- A. Gestión de Duplicados ---
df_clean = df_raw.drop_duplicates(subset=['CodProducto'], keep='first').copy()

# --- B. Limpieza de Espacios Invisibles (Deep Strip) ---
# Forzamos .astype(str) para evitar que falle si Pandas detectó números antes
for col in df_clean.columns:
    df_clean[col] = df_clean[col].astype(str).str.strip()

# --- C. Normalización de Textos (Capitalización) ---
for col in ['Descripcion', 'Categoria', 'Marca']:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].str.title()

# --- D. Limpieza "Deep" de Precios ---
# Solución al error de alineación y caracteres no numéricos
for col in ['PrecioCosto', 'PrecioVentaSugerido']:
    if col in df_clean.columns:
        # Forzamos a string, corregimos comas y convertimos a número flotante
        df_clean[col] = df_clean[col].astype(str).str.replace(',', '.')
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

# Rellenamos nulos numéricos resultantes de la limpieza
df_clean['PrecioCosto'] = df_clean['PrecioCosto'].fillna(0.0)
df_clean['PrecioVentaSugerido'] = df_clean['PrecioVentaSugerido'].fillna(0.0)

# --- E. Regla de Negocio: Margen de Ganancia ---
# Verificación de que el costo no supere la venta (consistencia financiera)
mask_error_precio = df_clean['PrecioCosto'] >= df_clean['PrecioVentaSugerido']
df_clean.loc[mask_error_precio, 'PrecioCosto'] = df_clean['PrecioVentaSugerido'] * 0.7

# ============================================================
# 4. MAPEO A ESTRUCTURA DIMENSIÓN
# ============================================================

columnas_dim = [
    "CodProducto", "Descripcion", "Categoria", "Marca", 
    "PrecioCosto", "PrecioVentaSugerido"
]

df_dim = df_clean.reindex(columns=columnas_dim)

# ============================================================
# 5. CARGA A SQL SERVER (DIM_PRODUCTO)
# ============================================================



try:
    # SQL genera el SK_Producto automáticamente por el IDENTITY(1,1)
    df_dim.to_sql("DIM_Producto", engine, if_exists="append", index=False)
    
    print("\n✅ --- REPORTE DE CALIDAD: PRODUCTOS ---")
    print(f"| SKU Originales: {total_inicial}")
    print(f"| SKU Únicos Cargados: {len(df_dim)}")
    print(f"| Precios con Limpieza String: Sí")
    print(f"| Inconsistencias Financieras Corregidas: {mask_error_precio.sum()}")
    print("----------------------------------------\n")
    
except Exception as e:
    print(f"❌ Error crítico en la carga: {e}")

# ============================================================
# 6. EXPORTACIÓN FINAL (CAPA 07)
# ============================================================
final_path = os.path.join(DEST_PATH, "Productos_DW_Final.csv")
df_dim.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"💾 Respaldo final guardado en: {final_path}")
print("====================================================")