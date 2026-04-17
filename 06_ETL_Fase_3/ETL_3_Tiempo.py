import pandas as pd
from sqlalchemy import create_engine

# 1. CONEXIÓN
engine = create_engine(r"mssql+pyodbc://DESKTOP-TPLKQNF\SQLSERVEREXP2019/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# 2. RUTA DEL ARCHIVO GENERADO (Las 15.000 ventas)
ruta = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop\05_INT\Datos_Historicos\Archivos_Generados\Ventas_generado.csv"

print("Leyendo archivo...")
df = pd.read_csv(ruta, sep=None, engine='python')

# 3. PROCESAR FECHAS ÚNICAS
df['FechaVenta'] = pd.to_datetime(df['FechaVenta'])
df_dim = df[['FechaVenta']].drop_duplicates().copy()
df_dim.columns = ['Tiempo_Key']
df_dim = df_dim.sort_values('Tiempo_Key')

# 4. CREAR ATRIBUTOS DE TIEMPO (Para tus reportes)
df_dim['Anio'] = df_dim['Tiempo_Key'].dt.year
df_dim['Mes'] = df_dim['Tiempo_Key'].dt.month
df_dim['Mes_Nombre'] = df_dim['Tiempo_Key'].dt.month_name()
df_dim['Semestre'] = df_dim['Mes'].apply(lambda x: 1 if x <= 6 else 2)
df_dim['Trimestre'] = df_dim['Tiempo_Key'].dt.quarter
df_dim['Semana_Anio'] = df_dim['Tiempo_Key'].dt.isocalendar().week
df_dim['Semana_Nro_Mes'] = (df_dim['Tiempo_Key'].dt.day - 1) // 7 + 1
df_dim['Dia'] = df_dim['Tiempo_Key'].dt.day
df_dim['Dia_Nombre'] = df_dim['Tiempo_Key'].dt.day_name()
df_dim['Dia_Semana_Nro'] = df_dim['Tiempo_Key'].dt.dayofweek + 1

# 5. CARGA LIMPIA A SQL
try:
    # Usamos append porque ya hicimos el TRUNCATE en SQL
    df_dim.to_sql("Dim_Tiempo", engine, if_exists="append", index=False)
    print(f"✅ ¡LISTO! Dim_Tiempo cargada con {len(df_dim)} días únicos.")
    print(f"Rango: {df_dim['Tiempo_Key'].min()} al {df_dim['Tiempo_Key'].max()}")
except Exception as e:
    print(f"❌ Error en la carga: {e}")