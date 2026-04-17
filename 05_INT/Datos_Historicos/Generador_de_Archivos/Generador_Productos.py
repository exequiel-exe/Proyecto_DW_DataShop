import pandas as pd
import numpy as np
import os

# ============================================================
# CONFIGURACIÓN DE RUTAS (INT)
# ============================================================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
output_path = os.path.join(
    BASE_PATH, "05_INT", "Datos_Historicos", "Archivos_Generados"
)
os.makedirs(output_path, exist_ok=True)

# ============================================================
# DEFINICIÓN DE PRODUCTOS BASE
# ============================================================

productos_definidos = [
    {"Categoria": "Televisores", "Descripcion": 'Televisor Led 55"', "CostoMin": 1500, "CostoMax": 2000},
    {"Categoria": "Televisores", "Descripcion": 'Televisor OLED 65"', "CostoMin": 1600, "CostoMax": 2800},
    {"Categoria": "Televisores", "Descripcion": "Smart TV 4K", "CostoMin": 1500, "CostoMax": 1800},
    {"Categoria": "Televisores", "Descripcion": 'Monitor 27"', "CostoMin": 1000, "CostoMax": 1500},

    {"Categoria": "Electrodomésticos", "Descripcion": "Refrigerador Doble Puerta", "CostoMin": 1500, "CostoMax": 2000},
    {"Categoria": "Electrodomésticos", "Descripcion": "Lavadora 8Kg", "CostoMin": 900, "CostoMax": 1400},
    {"Categoria": "Electrodomésticos", "Descripcion": "Microondas 20L", "CostoMin": 300, "CostoMax": 600},
    {"Categoria": "Electrodomésticos", "Descripcion": "Aspiradora Sin Bolsa", "CostoMin": 200, "CostoMax": 400},
    {"Categoria": "Electrodomésticos", "Descripcion": "Licuadora 600W", "CostoMin": 200, "CostoMax": 350},
    {"Categoria": "Electrodomésticos", "Descripcion": "Cafetera 12Tazas", "CostoMin": 220, "CostoMax": 450},

    {"Categoria": "Electrónica", "Descripcion": "Home Theater 5.1", "CostoMin": 1000, "CostoMax": 1500},
    {"Categoria": "Electrónica", "Descripcion": 'Tablet 10"', "CostoMin": 450, "CostoMax": 600},
    {"Categoria": "Electrónica", "Descripcion": "Smartwatch", "CostoMin": 280, "CostoMax": 450},
    {"Categoria": "Electrónica", "Descripcion": "Phone", "CostoMin": 500, "CostoMax": 1000},
    {"Categoria": "Electrónica", "Descripcion": "Auriculares Bluetooth", "CostoMin": 100, "CostoMax": 300},

    {"Categoria": "Computadoras", "Descripcion": 'Computadora Portátil 15"', "CostoMin": 1200, "CostoMax": 2000},
    {"Categoria": "Computadoras", "Descripcion": "PC de Escritorio", "CostoMin": 1000, "CostoMax": 1800},
    {"Categoria": "Computadoras", "Descripcion": "Laptop Gaming", "CostoMin": 1500, "CostoMax": 2200},
    {"Categoria": "Computadoras", "Descripcion": "All-in-One", "CostoMin": 1000, "CostoMax": 1800},
]

# ============================================================
# MARCAS POR CATEGORÍA
# ============================================================

marcas_por_categoria = {
    "Televisores": ["Samsung", "LG", "Sony", "Philips"],
    "Electrodomésticos": ["LG", "Whirlpool", "Panasonic", "Oster"],
    "Electrónica": ["Sony", "Apple", "Garmin", "Samsung"],
    "Computadoras": ["HP", "Dell", "Lenovo", "Apple"]
}

# ============================================================
# GENERACIÓN COHERENTE
# ============================================================

productos_generados = []
cod_producto = 1

# Cache de precios fijos por producto + marca
precios_por_producto_marca = {}

print("Generando productos coherentes...")

for prod in productos_definidos:
    for marca in marcas_por_categoria[prod["Categoria"]]:

        clave = (prod["Descripcion"], marca)

        if clave not in precios_por_producto_marca:
            # Precio base dentro del rango
            precio_base = np.random.randint(
                prod["CostoMin"], prod["CostoMax"] + 1
            )

            margen = np.random.uniform(0.15, 0.40)
            precio_sugerido = int(round(precio_base * (1 + margen)))

            # Ajuste para NO romper el rango
            if precio_sugerido > prod["CostoMax"]:
                precio_sugerido = prod["CostoMax"]

            precios_por_producto_marca[clave] = {
                "PrecioCosto": precio_base,
                "PrecioVentaSugerido": precio_sugerido
            }

        precios = precios_por_producto_marca[clave]

        productos_generados.append({
            "CodProducto": str(cod_producto),
            "Descripcion": prod["Descripcion"],
            "Categoria": prod["Categoria"],
            "Marca": marca,
            "PrecioCosto": str(precios["PrecioCosto"]),
            "PrecioVentaSugerido": str(precios["PrecioVentaSugerido"])
        })

        cod_producto += 1

# ============================================================
# EXPORT
# ============================================================

df_productos = pd.DataFrame(productos_generados)

output_file = os.path.join(output_path, "Productos_Generado.csv")
df_productos.to_csv(output_file, index=False)

print(f"Productos generados: {len(df_productos)}")
print("Archivo:", output_file)
print(df_productos.head())
