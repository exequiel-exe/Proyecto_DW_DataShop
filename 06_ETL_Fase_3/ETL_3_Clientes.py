import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

print("====================================================")
print("   ETL 3 – PROCESO DE ALTA CALIDAD: INT A DIM      ")
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
# 2. EXTRACCIÓN Y AUDITORÍA INICIAL
# ============================================================
csv_path = os.path.join(ORIG_PATH, "Clientes_generado.csv")

if not os.path.exists(csv_path):
    print(f"❌ Error: No se localizó el origen en {csv_path}")
    exit()

df_raw = pd.read_csv(csv_path, encoding="utf-8-sig")
total_inicial = len(df_raw)
print(f"📊 Registros recibidos de Integración: {total_inicial}")

# ============================================================
# 3. FILTROS DE CALIDAD Y SOFISTICACIÓN (TRANSFORMACIÓN)
# ============================================================

# --- A. Gestión de Duplicados de Negocio ---
# Si el sistema generó dos veces el mismo CodCliente, nos quedamos con el último
df_clean = df_raw.drop_duplicates(subset=['CodCliente'], keep='last').copy()
duplicados = total_inicial - len(df_clean)

# --- B. Enriquecimiento de Texto (Estandarización) ---
# Pasamos Nombres y Apellidos a formato "Title Case" (Ej: juan -> Juan)
for col in ['Nombre', 'Apellido', 'Localidad', 'Provincia']:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].astype(str).str.title()

# --- C. Validación de Rangos Éticos/Lógicos ---
# Si la edad es absurda (ej. < 0 o > 120), asignamos el promedio o valor nulo
df_clean.loc[(df_clean['Edad'] < 18) | (df_clean['Edad'] > 100), 'Edad'] = df_clean['Edad'].median()

# --- D. Formateo de Sexo (Normalización) ---
# Aseguramos que solo existan valores estándar para los filtros de Power BI
mapeo_sexo = {'H': 'Hombre', 'M': 'Mujer', 'F': 'Mujer', 'Otro': 'No Definido'}
df_clean['Sexo'] = df_clean['Sexo'].replace(mapeo_sexo).fillna('No Definido')

# ============================================================
# 4. PREPARACIÓN FINAL (ESTRUCTURA DW)
# ============================================================

columnas_dim = [
    "CodCliente", "Nombre", "Apellido", "Telefono", "Email", 
    "Direccion", "Localidad", "Provincia", "CP", "Sexo", "Edad"
]

# Reindexamos y forzamos tipos finales
df_dim = df_clean.reindex(columns=columnas_dim)
df_dim['CodCliente'] = df_dim['CodCliente'].astype(str)

# ============================================================
# 5. CARGA Y REPORTING
# ============================================================
try:
    # La SK_Cliente se genera automáticamente en SQL Server (Identity)
    df_dim.to_sql("DIM_Cliente", engine, if_exists="append", index=False)
    
    print("\n✅ --- REPORTE DE CALIDAD DE DATOS ---")
    print(f"| Registros Procesados: {total_inicial}")
    print(f"| Duplicados Eliminados: {duplicados}")
    print(f"| Registros Cargados en DW: {len(df_dim)}")
    print(f"| Edad Promedio Cargada: {df_dim['Edad'].mean():.1f} años")
    print("--------------------------------------\n")
    
except Exception as e:
    print(f"❌ Error crítico en la inyección al DW: {e}")

# ============================================================
# 6. EXPORTACIÓN A CAPA 07 (DATOS FINALES)
# ============================================================
final_path = os.path.join(DEST_PATH, "Clientes_DW_Final.csv")
df_dim.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"💾 Archivo de auditoría guardado: {final_path}")
print("====================================================")