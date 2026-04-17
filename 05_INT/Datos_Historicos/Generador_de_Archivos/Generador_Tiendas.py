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
# CONFIGURACIÓN DE RUTAS (INT)
# ============================================================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
output_path = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos")
os.makedirs(output_path, exist_ok=True)

# ============================================================
# GENERACIÓN DE TIENDAS
# ============================================================

CANT_TIENDAS = 15
tiendas_expandidas = []
used_descripciones = set()

print("Generando tiendas...")

for i in range(CANT_TIENDAS):

    tipo_tienda = np.random.choice(["Sucursal", "Online", "Outlet", "Express"])
    zona = np.random.choice(["Norte", "Sur", "Este", "Oeste", "Central", "Premium"])
    sufijo = np.random.choice(["", "Plus", "Max", "Pro", "Express"])

    descripcion = f"Tienda {zona} {tipo_tienda} {sufijo}".strip()
    while descripcion in used_descripciones:
        sufijo = np.random.choice(["", "Plus", "Max", "Pro", "Express"])
        descripcion = f"Tienda {zona} {tipo_tienda} {sufijo}".strip()

    used_descripciones.add(descripcion)

    calle_tipo = np.random.choice(["Calle", "Avenida", "Bulevar", "Ruta"])
    calle_nombre = np.random.choice(["Principal", "Central", "Secundaria", "Nueva", "Vieja"])
    numero = np.random.randint(1, 1000)
    direccion = f"{calle_tipo} {calle_nombre} {numero}"

    localidad = np.random.choice(list(localidad_provincia.keys()))
    provincia = localidad_provincia[localidad]
    cp = str(np.random.randint(10000, 100000))

    tiendas_expandidas.append({
        "CodTienda": str(i + 1),
        "Descripcion": descripcion,
        "Direccion": direccion,
        "Localidad": localidad,
        "Provincia": provincia,
        "CP": cp,
        "TipoTienda": tipo_tienda
    })

# ============================================================
# EXPORT
# ============================================================

df_tiendas_final = pd.DataFrame(tiendas_expandidas)
output_tiendas = os.path.join(output_path, "Tiendas_Generado.csv")
df_tiendas_final.to_csv(output_tiendas, index=False)

print(f"Tiendas generadas: {len(df_tiendas_final)}")
print("Archivo guardado en:", output_tiendas)
print("Ejemplo:")
print(df_tiendas_final.head())
