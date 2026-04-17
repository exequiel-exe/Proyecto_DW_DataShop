USE DW_DataShop;

CREATE TABLE INT_Fact_Ventas (
    FechaVenta     DATETIME     NOT NULL,
    CodProducto    VARCHAR(100) NOT NULL,
    Producto       VARCHAR(200) NULL,
    Cantidad       INT          NULL,
    PrecioVenta    DECIMAL(18,2) NULL,
    CodCliente     VARCHAR(100) NOT NULL,
    Cliente        VARCHAR(200) NULL,
    CodTienda      VARCHAR(100) NOT NULL,
    Tienda         VARCHAR(200) NULL,
    FechaCarga     DATETIME     NOT NULL DEFAULT (GETDATE())
);
