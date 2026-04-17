USE DW_DataShop

CREATE TABLE INT_Fact_Entregas (
    CodEntrega             INT NOT NULL,
    CodVenta               INT NOT NULL,
    Fecha_Envio            DATE NOT NULL,
    Fecha_Entrega_Estimada DATE NULL, -- Agregada para consistencia con el generador
    Fecha_Entrega_Real     DATE NULL,
    Tiempo_Entrega         VARCHAR(50) NULL,  -- Ejemplo: "5 días"
    Costo_Total_Entrega    DECIMAL(18,2) NULL,
    Entregado_A_Tiempo     VARCHAR(10) NULL,   -- "Si" / "No" / "Pendiente"
    Costo_Por_Km           DECIMAL(18,2) NULL,
    Distancia_Recorrida_Km DECIMAL(18,2) NULL,
    CodProveedor           INT NULL,
    CodRango               INT NULL,
    CodAlmacen             INT NULL,
    CodEstado              INT NULL,
    FechaCarga             DATETIME NOT NULL DEFAULT (GETDATE())
);