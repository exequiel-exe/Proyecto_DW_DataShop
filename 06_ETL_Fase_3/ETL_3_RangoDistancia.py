import pandas as pd
from sqlalchemy import create_engine, text
import os

print("====================================================")
print("      ETL PROFESIONAL: DIM_RANGODISTANCIA          ")
print("====================================================")

# 1. CONFIGURACIÓN DE RUTAS
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Origen según tu indicación
archivo_origen = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados", "Rango_Distancia.csv")
# Destino final
ruta_final_csv = os.path.join(BASE_PATH, "07_DW", "Datos_Finales", "Dim_RangoDistancia_Final.csv")

os.makedirs(os.path.dirname(ruta_final_csv), exist_ok=True)
engine = create_engine(r"mssql+pyodbc://DESKTOP-TPLKQNF\SQLSERVEREXP2019/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# 2. EXTRACT
if not os.path.exists(archivo_origen):
    # Si el archivo no existe físicamente, lo creamos basándonos en tu info para que el proceso no falle
    print("⚠️ Archivo no encontrado. Creando DataFrame con la información provista...")
    data = {
        'CodRango': [1, 2, 3],
        'TipoDistancia': ['Urbano', 'Regional', 'Larga Distancia'],
        'Km_Desde': [5, 51, 301],
        'Km_Hasta': [50, 300, 1000]
    }
    df = pd.DataFrame(data)
else:
    print("Leyendo archivo de origen...")
    df = pd.read_csv(archivo_origen, encoding="utf-8-sig")

# 3. TRANSFORM
print("Normalizando datos...")
df.columns = df.columns.str.strip()
df = df.drop_duplicates(subset=['CodRango']).dropna(subset=['CodRango'])
df['TipoDistancia'] = df['TipoDistancia'].str.strip()

# 4. LOAD A SQL SERVER
try:
    with engine.begin() as conn:
        print("Vaciando tabla Dim_RangoDistancia...")
        conn.execute(text("TRUNCATE TABLE Dim_RangoDistancia"))
    
    print("Cargando datos a SQL...")
    df.to_sql("Dim_RangoDistancia", engine, if_exists="append", index=False)
    
    # 5. GENERAR ARCHIVO FINAL CON SK
    df_final = pd.read_sql("SELECT * FROM Dim_RangoDistancia", engine)
    df_final.to_csv(ruta_final_csv, index=False, encoding="utf-8-sig")
    
    print(f"\n✅ PROCESO COMPLETADO EXITOSAMENTE")
    print(f"Archivo generado en: {ruta_final_csv}")

except Exception as e:
    print(f"❌ Error en la carga: {e}")

print("====================================================")