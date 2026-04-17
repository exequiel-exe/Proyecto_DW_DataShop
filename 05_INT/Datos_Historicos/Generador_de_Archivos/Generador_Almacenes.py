import pandas as pd
import numpy as np
import os

# ============================================================
# COHERENCIA LOCALIDAD - PROVINCIA
# ============================================================

localidad_provincia = {
    "Palo Alto": "California",
    "San Francisco": "California",
    "Los Angeles": "California",
    "Gotham City": "New Jersey",
    "New York": "New York",
    "Scranton": "Pennsylvania",
    "Springfield": "Illinois",
    "Shelbyville": "Illinois",
    "Capital City": "Illinois",
    "Sevastopol": "Alaska",
    "Raccoon City": "Illinois",
    "Silver City": "Illinois",
    "West City": "Illinois"
}

# ============================================================
# CONFIGURACIÓN PATH (INT)
# ============================================================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
output_path = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos")
os.makedirs(output_path, exist_ok=True)

CANT_ALMACENES = 15
almacenes = []
used_names = set()

print("Generando almacenes...")

# ============================================================
# GENERACIÓN DE DATOS
# ============================================================

for i in range(CANT_ALMACENES):

    tipo = np.random.choice(["Central", "Regional", "Logístico", "Distribución"])
    zona = np.random.choice(["Norte", "Sur", "Este", "Oeste", "Metropolitano"])
    nombre = f"Almacén {tipo} {zona}"

    while nombre in used_names:
        zona = np.random.choice(["Norte", "Sur", "Este", "Oeste", "Metropolitano"])
        nombre = f"Almacén {tipo} {zona}"

    used_names.add(nombre)

    localidad = np.random.choice(list(localidad_provincia.keys()))
    provincia = localidad_provincia[localidad]

    almacen = {
        "CodAlmacen": str(i + 1),
        "Nombre_Almacen": nombre,
        "Localidad": localidad,
        "Provincia": provincia
    }

    almacenes.append(almacen)

# ============================================================
# EXPORT
# ============================================================

df_almacenes = pd.DataFrame(almacenes)

output_file = os.path.join(output_path, "Almacenes_Generado.csv")
df_almacenes.to_csv(output_file, index=False, encoding="utf-8")

print(f"Almacenes generados: {len(df_almacenes)}")
print("Archivo guardado en:", output_file)
print("Ejemplo:")
print(df_almacenes.head())
