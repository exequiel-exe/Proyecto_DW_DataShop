import pandas as pd
from sqlalchemy import create_engine, text
import os
import numpy as np # Para generar las horas aleatorias

print("====================================================")
print("   ETL FINAL: FACT_VENTAS CON HORAS REALISTAS      ")
print("====================================================")

# 1. CONFIGURACIÓN
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
archivo_ventas = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados", "Ventas_generado.csv")
ruta_final_csv = os.path.join(BASE_PATH, "07_DW", "Datos_Finales", "FACT_Ventas_Final.csv")

os.makedirs(os.path.dirname(ruta_final_csv), exist_ok=True)
engine = create_engine(r"mssql+pyodbc://DESKTOP-TPLKQNF\SQLSERVEREXP2019/DW_DataShop?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes")

# 2. EXTRACCIÓN
df_ventas = pd.read_csv(archivo_ventas, sep=None, engine='python', encoding="utf-8-sig")
df_ventas.columns = df_ventas.columns.str.replace('^\ufeff', '', regex=True).str.strip()

# 3. TRANSFORMACIÓN + GENERACIÓN DE HORAS ALEATORIAS
print("Limpiando datos y distribuyendo horas de venta...")

# Convertimos a fecha base (sin hora útil aún)
df_ventas['FechaVenta'] = pd.to_datetime(df_ventas['FechaVenta']).dt.normalize()

# Generamos un desplazamiento aleatorio (horas, minutos, segundos) para cada fila
# Rango: desde las 08:00 hasta las 21:00 (horario comercial típico)
horas_random = pd.to_timedelta(np.random.randint(8, 21, size=len(df_ventas)), unit='h')
minutos_random = pd.to_timedelta(np.random.randint(0, 60, size=len(df_ventas)), unit='m')
segundos_random = pd.to_timedelta(np.random.randint(0, 60, size=len(df_ventas)), unit='s')

# Sumamos los desplazamientos a la fecha base
df_ventas['FechaVenta'] = df_ventas['FechaVenta'] + horas_random + minutos_random + segundos_random

# Limpieza de métricas
df_ventas['Cantidad'] = pd.to_numeric(df_ventas['Cantidad'], errors='coerce').fillna(0).astype(int)
df_ventas['PrecioVenta'] = pd.to_numeric(df_ventas['PrecioVenta'], errors='coerce').fillna(0.0).astype(float)

# 4. DIMENSIONES (SK)
df_prod_sk = pd.read_sql("SELECT * FROM Dim_Producto", engine)
df_clie_sk = pd.read_sql("SELECT * FROM Dim_Cliente", engine)
df_tien_sk = pd.read_sql("SELECT * FROM Dim_Tienda", engine)

# Normalizar códigos a String
for df in [df_ventas, df_prod_sk, df_clie_sk, df_tien_sk]:
    for col in ['CodProducto', 'CodCliente', 'CodTienda']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

col_prod_sk, col_clie_sk, col_tien_sk = df_prod_sk.columns[0], df_clie_sk.columns[0], df_tien_sk.columns[0]

# 5. INTEGRACIÓN
df_final = pd.merge(df_ventas, df_prod_sk[[col_prod_sk, 'CodProducto']], on='CodProducto', how='inner')
df_final = pd.merge(df_final, df_clie_sk[[col_clie_sk, 'CodCliente']], on='CodCliente', how='inner')
df_final = pd.merge(df_final, df_tien_sk[[col_tien_sk, 'CodTienda']], on='CodTienda', how='inner')

# 6. SELECCIÓN FINAL
df_final = df_final[[col_clie_sk, col_prod_sk, col_tien_sk, 'FechaVenta', 'Cantidad', 'PrecioVenta']]
df_final.columns = ['SK_Cliente', 'SK_Producto', 'SK_Tienda', 'FechaVenta', 'Cantidad', 'PrecioVenta']

# 7. CARGA SQL Y CSV
try:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE FACT_Ventas"))
    
    df_final.to_sql("FACT_Ventas", engine, if_exists="append", index=False, chunksize=1000)
    df_final.to_csv(ruta_final_csv, index=False, encoding="utf-8-sig")

    print("\n✅ ETL COMPLETADO CON ÉXITO")
    print(f"Total registros: {len(df_final)}")
    print(f"Muestra de fecha con hora: {df_final['FechaVenta'].iloc[0]}")

except Exception as e:
    print(f"❌ Error: {e}")