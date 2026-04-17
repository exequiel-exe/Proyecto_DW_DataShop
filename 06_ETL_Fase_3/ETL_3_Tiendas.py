import os
import pandas as pd
from sqlalchemy import create_engine

print("====================================================")
print("   ETL 3 – CALIDAD PRO: DIM_TIENDA (INT A DIM)     ")
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
csv_path = os.path.join(ORIG_PATH, "Tiendas_generado.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se localizó el archivo en {csv_path}")
    exit()

df_raw = pd.read_csv(csv_path, encoding="utf-8-sig")
total_inicial = len(df_raw)

# ============================================================
# 3. TRANSFORMACIÓN Y SOFISTICACIÓN (GEOGRÁFICA)
# ============================================================

# --- A. Gestión de Duplicados ---
df_clean = df_raw.drop_duplicates(subset=['CodTienda'], keep='first').copy()

# --- B. Limpieza masiva de espacios (Deep Strip) ---
for col in df_clean.columns:
    df_clean[col] = df_clean[col].astype(str).str.strip()

# --- C. Estandarización de Geografía y Tipo ---
# Pasamos a Formato Título para que los mapas de Power BI agrupen bien
for col in ['Descripcion', 'Localidad', 'Provincia', 'TipoTienda']:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].str.title()

# --- D. Tratamiento de Valores Nulos ---
df_clean['TipoTienda'] = df_clean['TipoTienda'].replace('Nan', 'Física').fillna('Física')
df_clean['Localidad'] = df_clean['Localidad'].replace('Nan', 'No Definida').fillna('No Definida')

# ============================================================
# 4. MAPEO A ESTRUCTURA DIMENSIÓN
# ============================================================

columnas_dim = [
    "CodTienda", "Descripcion", "Direccion", "Localidad", 
    "Provincia", "CP", "TipoTienda"
]

df_dim = df_clean.reindex(columns=columnas_dim)

# ============================================================
# 5. CARGA A SQL SERVER (DIM_TIENDA)
# ============================================================


try:
    # SQL genera el SK_Tienda automáticamente (Identity)
    df_dim.to_sql("DIM_Tienda", engine, if_exists="append", index=False)
    
    print("\n✅ --- REPORTE DE CALIDAD: TIENDAS ---")
    print(f"| Tiendas Originales: {total_inicial}")
    print(f"| Tiendas Únicas Cargadas: {len(df_dim)}")
    print(f"| Limpieza de Cadenas: Completada")
    print("--------------------------------------\n")
    
except Exception as e:
    print(f"❌ Error crítico en la carga: {e}")

# ============================================================
# 6. EXPORTACIÓN FINAL (CAPA 07)
# ============================================================
final_path = os.path.join(DEST_PATH, "Tiendas_DW_Final.csv")
df_dim.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"💾 Respaldo de Tiendas guardado en: {final_path}")
print("====================================================")