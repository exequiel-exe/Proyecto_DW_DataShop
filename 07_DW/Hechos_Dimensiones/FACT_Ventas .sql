USE DW_DataShop

CREATE TABLE FACT_Ventas (

    VentaSK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SK_Cliente  INT NOT NULL,
    SK_Producto INT NOT NULL,
    SK_Tienda   INT NOT NULL,
    FechaVenta DATETIME NOT NULL,
    Cantidad   INT NOT NULL,
    PrecioVenta DECIMAL(18,2) NOT NULL,
    FechaCarga DATETIME NOT NULL DEFAULT (GETDATE())

);
