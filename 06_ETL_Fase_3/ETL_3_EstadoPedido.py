import pandas as pd
from sqlalchemy import create_engine, text
import os

print("====================================================")
print("      ETL PROFESIONAL: DIM_ESTADOPEDIDO             ")
print("====================================================")

# 1. CONFIGURACIÓN DE RUTAS
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
archivo_origen = os.path.join(BASE_PATH, "05_INT", "Archivos_Limpios", "EstadoPedido_INT.csv")
ruta_final_csv = os.path.join(BASE_PATH, "07_DW", "Datos_Finales", "Dim_EstadoPedido_Final.csv")

# Configuración de conexión SQL
engine = create_engine(r"mssql+pyodbc://DESKTOP-TPLKQNF\SQLSERVEREXP2019/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# 2. EXTRACT
if not os.path.exists(archivo_origen):
    print(f"❌ Error: No se encuentra el archivo en {archivo_origen}")
    exit()

print("Leyendo datos de INT (Archivos_Limpios)...")
df = pd.read_csv(archivo_origen, encoding="utf-8-sig")

# 3. TRANSFORM
print("Aplicando transformaciones profesionales...")
# Limpiar nombres de columnas y espacios
df.columns = df.columns.str.strip()
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Eliminar duplicados para asegurar que sea una dimensión pura
df = df.drop_duplicates(subset=['CodEstado']).dropna(subset=['CodEstado'])

# Asegurar tipos de datos
df['CodEstado'] = df['CodEstado'].astype(int)
df['Descripcion_Estado'] = df['Descripcion_Estado'].str.upper()

# 4. LOAD A SQL SERVER
try:
    with engine.begin() as conn:
        print("Vaciando tabla Dim_EstadoPedido en SQL...")
        conn.execute(text("TRUNCATE TABLE Dim_EstadoPedido"))
    
    print("Cargando datos en la base de datos...")
    # No enviamos el SK porque es IDENTITY (se genera solo)
    df.to_sql("Dim_EstadoPedido", engine, if_exists="append", index=False)
    
    # 5. GENERAR ARCHIVO FINAL (Backup con SKs reales)
    # Leemos de nuevo para incluir los SK generados por SQL
    df_final = pd.read_sql("SELECT * FROM Dim_EstadoPedido", engine)
    
    print(f"Generando archivo final en: {ruta_final_csv}")
    df_final.to_csv(ruta_final_csv, index=False, encoding="utf-8-sig")

    print("\n✅ PROCESO COMPLETADO EXITOSAMENTE")
    print(f"Total estados registrados: {len(df_final)}")

except Exception as e:
    print(f"❌ Error durante la carga: {e}")

print("====================================================")