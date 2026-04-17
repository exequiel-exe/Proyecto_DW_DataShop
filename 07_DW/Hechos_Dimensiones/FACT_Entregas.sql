USE DW_DataShop;
GO

IF OBJECT_ID('Fact_Entregas', 'U') IS NOT NULL 
    DROP TABLE FACT_Entregas;
GO

CREATE TABLE FACT_Entregas (
    EntregaSK           INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Claves Foráneas
    VentaSK             INT NOT NULL,           
    SK_Proveedor        INT NOT NULL,           
    SK_RangoDistancia   INT NOT NULL,           
    SK_EstadoPedido     INT NOT NULL,           
    SK_Almacen          INT NOT NULL,  -- <--- COLUMNA AGREGADA

    -- Atributos de Fecha
    Fecha_Envio         DATE NOT NULL,
    Fecha_Estimada      DATE NOT NULL,
    Fecha_Entrega_Real  DATE NULL,
    
    -- Hechos / Métricas
    Distancia_Km        DECIMAL(10,2) NOT NULL,
    Costo_Total         DECIMAL(10,2) NOT NULL,
    Costo_Por_Km        DECIMAL(10,4) NOT NULL,
    Tiempo_Entrega_Dias INT NULL,               
    Entregado_A_Tiempo  NVARCHAR(15) NOT NULL, -- <--- Aumentado a 15 para "Pendiente"
    
    FechaCarga          DATETIME DEFAULT GETDATE(),

    -- Definición de Relaciones
    CONSTRAINT FK_Entregas_Venta FOREIGN KEY (VentaSK) REFERENCES FACT_Ventas(VentaSK),
    CONSTRAINT FK_Entregas_Proveedor FOREIGN KEY (SK_Proveedor) REFERENCES DIM_Proveedores(SK_Proveedor),
    CONSTRAINT FK_Entregas_Rango FOREIGN KEY (SK_RangoDistancia) REFERENCES DIM_RangoDistancia(SK_RangoDistancia),
    CONSTRAINT FK_Entregas_Estado FOREIGN KEY (SK_EstadoPedido) REFERENCES DIM_EstadoPedido(SK_EstadoPedido),
    CONSTRAINT FK_Entregas_Almacen FOREIGN KEY (SK_Almacen) REFERENCES DIM_Almacen(SK_Almacen) -- <--- RELACIÓN AGREGADA
);
GO