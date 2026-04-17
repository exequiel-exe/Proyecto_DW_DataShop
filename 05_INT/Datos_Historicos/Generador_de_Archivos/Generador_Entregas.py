import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

# ============================================================
# 1. CONFIGURACIÓN DE RUTAS Y DICCIONARIOS
# ============================================================
BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
DIM_PATH = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados")

ventas_file = os.path.join(DIM_PATH, "Ventas_generado.csv")
clientes_file = os.path.join(DIM_PATH, "Clientes_generado.csv") # <--- NECESARIO PARA LOCALIDAD
output_entregas = os.path.join(DIM_PATH, "Entregas_generado.csv")

# --- NUEVA LÓGICA DE ALMACENES ---
loc_to_prov = {
    "Palo Alto": "California", "San Francisco": "California", "Los Angeles": "California",
    "Gotham City": "New Jersey", "New York": "New York", "Scranton": "Pennsylvania",
    "Springfield": "Illinois", "Shelbyville": "Illinois", "Capital City": "Illinois",
    "Sevastopol": "Alaska", "Raccoon City": "Illinois", "Silver City": "Illinois", "West City": "Illinois"
}

almacenes_por_prov = {
    "Alaska": [1], "Illinois": [2, 4, 6, 8, 14, 15], "California": [3, 7, 11, 12],
    "Pennsylvania": [5, 10], "New York": [9, 13], "New Jersey": [6]
}

# Diccionario de Proveedores con CodRango integrado (RN-04)
proveedores_dict = {
    1: {"Costo_Base": 200, "Km_Incl": 5, "Tarifa": 6.0, "CodRango": 1}, 
    2: {"Costo_Base": 230, "Km_Incl": 5, "Tarifa": 6.5, "CodRango": 1},
    3: {"Costo_Base": 260, "Km_Incl": 5, "Tarifa": 7.0, "CodRango": 1},
    4: {"Costo_Base": 500, "Km_Incl": 50, "Tarifa": 4.0, "CodRango": 2}, 
    5: {"Costo_Base": 550, "Km_Incl": 50, "Tarifa": 4.5, "CodRango": 2},
    6: {"Costo_Base": 600, "Km_Incl": 50, "Tarifa": 5.0, "CodRango": 2},
    7: {"Costo_Base": 1200, "Km_Incl": 300, "Tarifa": 2.0, "CodRango": 3}, 
    8: {"Costo_Base": 1300, "Km_Incl": 300, "Tarifa": 2.3, "CodRango": 3}
}

if not os.path.exists(ventas_file) or not os.path.exists(clientes_file):
    print(f"❌ Error: Faltan archivos base en {DIM_PATH}")
    exit()

df_ventas = pd.read_csv(ventas_file)
df_clientes = pd.read_csv(clientes_file)
df_ventas['FechaVenta'] = pd.to_datetime(df_ventas['FechaVenta'])

# Unimos ventas con clientes para saber de donde es cada comprador
df_mapeo_ventas = df_ventas.merge(df_clientes[['CodCliente', 'Localidad']], on='CodCliente', how='left')

entregas_list = []

print("🚀 Generando entregas con Lógica de Almacenes Regionales...")

# ============================================================
# 2. BUCLE DE GENERACIÓN
# ============================================================
for index, venta in df_mapeo_ventas.iterrows():
    
    # --- PASO A: SELECCIÓN DE PROVEEDOR POR CARGA ---
    cant = venta['Cantidad']
    if cant <= 2:
        ids_posibles = [1, 2, 3]
    elif cant <= 4:
        ids_posibles = [4, 5, 6]
    else:
        ids_posibles = [7, 8]
    
    id_p = np.random.choice(ids_posibles)
    p = proveedores_dict[id_p]
    cod_rango = p['CodRango']

    # --- PASO B: GENERACIÓN DE DISTANCIA (RN-04) ---
    if cod_rango == 1:
        distancia = round(np.random.uniform(5, 50), 2)
    elif cod_rango == 2:
        distancia = round(np.random.uniform(51, 300), 2)
    else:
        distancia = round(np.random.uniform(301, 1500), 2)

    # --- PASO C: CÁLCULO DE COSTOS (RN-02 y RN-05) ---
    dist_excedente = max(0, distancia - p['Km_Incl'])
    costo_total = round(p['Costo_Base'] + (dist_excedente * p['Tarifa']), 2)
    costo_km = round(costo_total / distancia, 2)

    # --- PASO D: LOGÍSTICA DE TIEMPOS Y FECHAS (RN-01 y RN-03) ---
    cod_estado = np.random.choice([1, 2, 3, 4], p=[0.05, 0.05, 0.85, 0.05])
    f_envio = venta['FechaVenta'] + timedelta(days=np.random.randint(0, 3))
    f_estimada = f_envio + timedelta(days=4)
    
    entrega_real_str = None
    t_entrega = "Pendiente"
    a_tiempo = "Pendiente"

    if cod_estado in [3, 4]:
        f_entrega_real = f_envio + timedelta(days=np.random.randint(1, 8))
        entrega_real_str = f_entrega_real.strftime("%Y-%m-%d")
        diff_dias = (f_entrega_real - f_envio).days
        t_entrega = f"{diff_dias} días"
        a_tiempo = "Si" if f_entrega_real <= f_estimada else "No"

    # --- PASO E: ASIGNACIÓN DE ALMACÉN (NUEVO) ---
    provincia = loc_to_prov.get(venta['Localidad'], "Illinois") # Por defecto Illinois
    cod_almacen = random.choice(almacenes_por_prov.get(provincia, [6]))

    # --- PASO F: ALMACENAMIENTO ---
    entregas_list.append({
        "CodEntrega": index + 1,
        "CodVenta": venta['CodVenta'],
        "Fecha_Envio": f_envio.strftime("%Y-%m-%d"),
        "Fecha_Estimada": f_estimada.strftime("%Y-%m-%d"),
        "Fecha_Entrega_Real": entrega_real_str,
        "Tiempo_Entrega": t_entrega,
        "Costo_Total_Entrega": costo_total,
        "Entregado_A_Tiempo": a_tiempo,
        "Costo_Por_Km": costo_km,
        "Distancia_Recorrida_Km": distancia,
        "CodProveedor": id_p,
        "CodRango": cod_rango,
        "CodEstado": cod_estado,
        "CodAlmacen": cod_almacen  # <--- NUEVA COLUMNA ASIGNADA
    })

# ============================================================
# 3. GUARDADO FINAL
# ============================================================
df_final = pd.DataFrame(entregas_list)
df_final.to_csv(output_entregas, index=False, encoding="utf-8-sig")

print("\n" + "="*45)
print("✅ GENERACIÓN EXITOSA CON ALMACENES")
print("="*45)
print(f"| Destino: {output_entregas}")
print(f"| Almacenes asignados correctamente.")
print("-" * 45)
print(df_final[['CodVenta', 'CodAlmacen', 'Entregado_A_Tiempo']].head(5))
print("="*45)