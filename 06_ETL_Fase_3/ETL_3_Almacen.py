import pandas as pd
from sqlalchemy import create_engine, text
import os

print("====================================================")
print("        ETL PROFESIONAL: DIM_ALMACEN               ")
print("====================================================")

# 1. CONFIGURACIÓN DE RUTAS
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
archivo_origen = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados", "Almacenes_Generado.csv")
ruta_final_csv = os.path.join(BASE_PATH, "07_DW", "Datos_Finales", "Dim_Almacen_Final.csv")

engine = create_engine(r"mssql+pyodbc://DESKTOP-TPLKQNF\SQLSERVEREXP2019/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# 2. EXTRACT
if not os.path.exists(archivo_origen):
    print(f"❌ Error: No se encontró el archivo en {archivo_origen}")
    exit()

print("Leyendo datos de origen...")
df = pd.read_csv(archivo_origen, encoding="utf-8-sig")

# 3. TRANSFORM
print("Limpiando y estandarizando datos de Almacenes...")
df.columns = df.columns.str.strip()

# Eliminamos duplicados por código de almacén
df = df.drop_duplicates(subset=['CodAlmacen']).dropna(subset=['CodAlmacen'])

# Limpieza de strings: quitar espacios y pasar a mayúsculas para reportes limpios
columnas_texto = ['Nombre_Almacen', 'Localidad', 'Provincia']
for col in columnas_texto:
    if col in df.columns:
        df[col] = df[col].str.strip().str.upper()

# 4. LOAD A SQL SERVER
try:
    with engine.begin() as conn:
        print("Vaciando tabla Dim_Almacen en SQL...")
        conn.execute(text("TRUNCATE TABLE Dim_Almacen"))
    
    print("Cargando datos a la dimensión...")
    df.to_sql("Dim_Almacen", engine, if_exists="append", index=False)
    
    # 5. GENERAR ARCHIVO FINAL CON SKs
    df_final = pd.read_sql("SELECT * FROM Dim_Almacen", engine)
    df_final.to_csv(ruta_final_csv, index=False, encoding="utf-8-sig")

    print("\n✅ ETL DE ALMACENES FINALIZADO")
    print(f"Almacenes procesados: {len(df_final)}")
    print(f"Archivo guardado en: {ruta_final_csv}")

except Exception as e:
    print(f"❌ Error en el proceso: {e}")

print("====================================================")