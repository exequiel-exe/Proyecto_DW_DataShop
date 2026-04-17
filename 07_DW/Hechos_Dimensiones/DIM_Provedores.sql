USE DW_DataShop;

CREATE TABLE DIM_Proveedores (
    SK_Proveedor      INT IDENTITY(1,1) PRIMARY KEY,
    CodProveedor     INT NOT NULL,
    Nombre_Proveedor NVARCHAR(100),
    Costo_Base       DECIMAL(10, 2),
    Km_Incluidos     INT,
    Tarifa_Km       DECIMAL(10, 2),
    SK_RangoDistancia INT NOT NULL, -- <--- Nombre exacto de tu columna
    FechaCarga       DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT FK_Proveedor_Rango 
    FOREIGN KEY (SK_RangoDistancia) REFERENCES DIM_RangoDistancia(SK_RangoDistancia)
);
