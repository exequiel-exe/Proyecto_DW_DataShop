import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Carpeta donde están tus dimensiones ya generadas
DIM_PATH = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados")

clientes_file  = os.path.join(DIM_PATH, "Clientes_generado.csv")
productos_file = os.path.join(DIM_PATH, "Productos_generado.csv")
tiendas_file   = os.path.join(DIM_PATH, "Tiendas_generado.csv")
output_ventas  = os.path.join(DIM_PATH, "Ventas_generado.csv")

def load_csv_safe(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Error: No se encontró el archivo {path}. Asegúrate de haber corrido los generadores previos.")
    return pd.read_csv(path)

# Carga de archivos maestros
df_clientes = load_csv_safe(clientes_file)
df_productos = load_csv_safe(productos_file)
df_tiendas = load_csv_safe(tiendas_file)

# ============================================================
# 2. PARÁMETROS DE GENERACIÓN
# ============================================================
CANT_VENTAS = 15000
start_date = datetime(2021, 1, 1)
end_date   = datetime(2023, 12, 31)

# Convertimos a listas de diccionarios (mucho más rápido para iterar)
productos_list = df_productos.to_dict("records")
clientes_list = df_clientes.to_dict("records")
tiendas_list = df_tiendas.to_dict("records")

ventas_generadas = []

print(f"Iniciando generación de {CANT_VENTAS} ventas...")

# ============================================================
# 3. BUCLE DE GENERACIÓN (LÓGICA DE NEGOCIO)
# ============================================================
for _ in range(CANT_VENTAS):
    # Selección aleatoria de entidades
    prod = np.random.choice(productos_list)
    cli = np.random.choice(clientes_list)
    tie = np.random.choice(tiendas_list)
    
    # RN 1: Fecha aleatoria dentro del rango de 3 años
    dias_rango = (end_date - start_date).days
    fecha_aleatoria = start_date + timedelta(days=np.random.randint(0, dias_rango))

    # RN 2: Lógica de Precio Dinámica (Basada en Sugerido)
    p_costo = float(prod["PrecioCosto"])
    p_sugerido = float(prod["PrecioVentaSugerido"])
    
    # Fluctración de mercado: entre -5% y +10% del sugerido
    variacion = np.random.uniform(-0.05, 0.10)
    precio_final = round(p_sugerido * (1 + variacion), 2)
    
    # RN 3: Validación de Margen (Seguridad para no vender a pérdida)
    if precio_final <= p_costo:
        precio_final = round(p_costo * 1.15, 2) 

    # RN 4: Cantidad Ponderada (Probabilidad de compra normalizada)
    cantidad = np.random.choice([1, 2, 3, 4, 5], p=[0.6, 0.2, 0.1, 0.05, 0.05])

    venta = {
        "FechaVenta": fecha_aleatoria, # Guardamos como objeto datetime para ordenar
        "CodProducto": prod["CodProducto"],
        "Producto": prod["Descripcion"],
        "Cantidad": cantidad,
        "PrecioVenta": precio_final,
        "CodCliente": cli["CodCliente"],
        "Cliente": f"{cli['Nombre']} {cli['Apellido']}",
        "CodTienda": tie["CodTienda"],
        "Tienda": tie["Descripcion"]
    }
    ventas_generadas.append(venta)

# ============================================================
# 4. ORDENAMIENTO Y ASIGNACIÓN DE ID (CRÍTICO)
# ============================================================
# Creamos el DataFrame
df_final = pd.DataFrame(ventas_generadas)

# Ordenamos por fecha de la más antigua a la más reciente
df_final.sort_values("FechaVenta", inplace=True)

# Asignamos el CodVenta secuencial (1, 2, 3...) según el orden de la fecha
df_final.insert(0, "CodVenta", range(1, len(df_final) + 1))

# Convertimos la fecha a string solo para el guardado final (formato limpio)
df_final["FechaVenta"] = df_final["FechaVenta"].dt.strftime("%Y-%m-%d")

# ============================================================
# 5. GUARDADO
# ============================================================
df_final.to_csv(output_ventas, index=False, encoding="utf-8-sig")

print(f"✔ ¡Éxito! Archivo generado con {len(df_final)} registros.")
print(f"✔ Ubicación: {output_ventas}")
print("\nPrimeras 5 ventas del histórico (Ordenadas por Fecha e ID):")
print(df_final.head())