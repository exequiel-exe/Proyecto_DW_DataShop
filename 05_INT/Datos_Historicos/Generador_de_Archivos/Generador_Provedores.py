import pandas as pd
import os

# ==============================
# Configuración CORRECTA
# ==============================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
# Ajusté la ruta para que coincida con tu estructura de carpetas
output_path = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados")
os.makedirs(output_path, exist_ok=True)

proveedores = [
    # =====================
    # URBANO (CodRango: 1)
    # =====================
    {"CodProveedor": 1, "Proveedor": "EcoDelivery",
     "Costo_Base": 200, "Km_Incluidos": 5, "Tarifa_Km": 6.0, "CodRango": 1},

    {"CodProveedor": 2, "Proveedor": "UrbanMove",
     "Costo_Base": 230, "Km_Incluidos": 5, "Tarifa_Km": 6.5, "CodRango": 1},

    {"CodProveedor": 3, "Proveedor": "QuickDelivery",
     "Costo_Base": 260, "Km_Incluidos": 5, "Tarifa_Km": 7.0, "CodRango": 1},

    # =====================
    # REGIONAL (CodRango: 2)
    # =====================
    {"CodProveedor": 4, "Proveedor": "RegionalExpress",
     "Costo_Base": 500, "Km_Incluidos": 50, "Tarifa_Km": 4.0, "CodRango": 2},

    {"CodProveedor": 5, "Proveedor": "FastShip",
     "Costo_Base": 550, "Km_Incluidos": 50, "Tarifa_Km": 4.5, "CodRango": 2},

    {"CodProveedor": 6, "Proveedor": "PrimeCourier",
     "Costo_Base": 600, "Km_Incluidos": 50, "Tarifa_Km": 5.0, "CodRango": 2},

    # =====================
    # LARGA DISTANCIA (CodRango: 3)
    # =====================
    {"CodProveedor": 7, "Proveedor": "GlobalShip",
     "Costo_Base": 1200, "Km_Incluidos": 300, "Tarifa_Km": 2.0, "CodRango": 3},

    {"CodProveedor": 8, "Proveedor": "ExpressLog",
     "Costo_Base": 1300, "Km_Incluidos": 300, "Tarifa_Km": 2.3, "CodRango": 3}
]

# Crear DataFrame
df_proveedores = pd.DataFrame(proveedores)

# Guardar Archivo
output_file = os.path.join(output_path, "Proveedores_generado.csv")
df_proveedores.to_csv(output_file, index=False, encoding="utf-8-sig")

print("✔ Proveedores generados con Clave Foránea 'CodRango' integrada.")
print(f"Archivo creado en: {output_file}")
print("-" * 30)
print(df_proveedores[['CodProveedor', 'Proveedor', 'CodRango']])