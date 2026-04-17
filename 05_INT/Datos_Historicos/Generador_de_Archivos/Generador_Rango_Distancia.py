import pandas as pd
import os

# ==============================
# Configuración
# ==============================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
output_path = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos")
os.makedirs(output_path, exist_ok=True)

# ==============================
# Definición de Rangos de Distancia
# Reglas:
# Urbano          -> 5  <= km <= 50
# Regional        -> 50 <  km <= 300
# Larga Distancia -> 300 < km <= 1000
# ==============================
rangos = [
    {
        "CodRango": 1,
        "TipoDistancia": "Urbano",
        "Km_Desde": 5,
        "Km_Hasta": 50
    },
    {
        "CodRango": 2,
        "TipoDistancia": "Regional",
        "Km_Desde": 51,
        "Km_Hasta": 300
    },
    {
        "CodRango": 3,
        "TipoDistancia": "Larga Distancia",
        "Km_Desde": 301,
        "Km_Hasta": 1000
    }
]

df_rangos = pd.DataFrame(rangos)

# ==============================
# Export
# ==============================
output_file = os.path.join(output_path, "RangosDistancia_generado.csv")
df_rangos.to_csv(output_file, index=False, encoding="utf-8-sig")

print("✔ Rangos de distancia generados correctamente")
print(f"Archivo creado en: {output_file}")
print(df_rangos)
