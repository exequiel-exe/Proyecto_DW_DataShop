import pandas as pd
import numpy as np
import os

# ============================================================
# CONFIGURACIÓN DE RUTAS (INT)
# ============================================================

BASE_PATH = r"C:\Users\exequ\OneDrive\Escritorio\Proyecto_DW_DataShop"
output_path = os.path.join(BASE_PATH, "05_INT", "Datos_Historicos")
os.makedirs(output_path, exist_ok=True)

# ============================================================
# RELACIÓN LOCALIDAD - PROVINCIA (RN EXPLÍCITA)
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
# LISTAS DE DATOS
# ============================================================

nombres_masculinos = [
    "Juan", "Carlos", "Luis", "Miguel", "José", "Antonio", "Francisco",
    "Javier", "David", "Alejandro", "Daniel", "Pablo", "Manuel", "Pedro", "Ángel"
]

nombres_femeninos = [
    "María", "Ana", "Laura", "Carmen", "Isabel", "Pilar", "Dolores",
    "Teresa", "Cristina", "Mónica", "Rosa", "Patricia", "Mercedes",
    "Concepción", "Lucía"
]

apellidos = [
    "García", "Rodríguez", "González", "Fernández", "López", "Martínez",
    "Sánchez", "Pérez", "Martín", "Ruiz", "Hernández", "Jiménez",
    "Díaz", "Moreno", "Álvarez"
]

# ============================================================
# GENERACIÓN DE CLIENTES
# ============================================================

CANT_CLIENTES = 100
clientes_expandidos = []
used_telefonos = set()

print("Generando clientes...")

for i in range(CANT_CLIENTES):
    sexo = np.random.choice(["Masculino", "Femenino"])
    nombre = np.random.choice(
        nombres_masculinos if sexo == "Masculino" else nombres_femeninos
    )
    apellido = np.random.choice(apellidos)

    edad = np.random.randint(18, 71)

    calle_tipo = np.random.choice(["Calle", "Avenida", "Bulevar", "Ruta"])
    calle_nombre = np.random.choice(["Principal", "Central", "Secundaria", "Nueva", "Vieja"])
    numero = np.random.randint(1, 1000)
    direccion = f"{calle_tipo} {calle_nombre} {numero}"

    # ========================================================
    # LOCALIDAD Y PROVINCIA (RESPETA RN)
    # ========================================================

    localidad = np.random.choice(list(localidad_provincia.keys()))
    provincia = localidad_provincia[localidad]

    cp = str(np.random.randint(10000, 100000))

    telefono = str(np.random.randint(100000000, 999999999))
    while telefono in used_telefonos:
        telefono = str(np.random.randint(100000000, 999999999))
    used_telefonos.add(telefono)

    email = (
        f"{nombre.lower()}.{apellido.lower()}{np.random.randint(10,999)}@"
        f"{np.random.choice(['gmail.com', 'hotmail.com', 'yahoo.com'])}"
    )

    clientes_expandidos.append({
        "CodCliente": str(i + 1),
        "Nombre": nombre,
        "Apellido": apellido,
        "Telefono": telefono,
        "Email": email,
        "Direccion": direccion,
        "Localidad": localidad,
        "Provincia": provincia,
        "CP": cp,
        "Sexo": sexo,
        "Edad": str(edad)
    })

# ============================================================
# EXPORT
# ============================================================

df_clientes_final = pd.DataFrame(clientes_expandidos)
output_clientes = os.path.join(output_path, "Clientes_Generado.csv")
df_clientes_final.to_csv(output_clientes, index=False)

print(f"Clientes generados: {len(df_clientes_final)}")
print("Archivo guardado en:", output_clientes)
print("Ejemplo:")
print(df_clientes_final.head())
