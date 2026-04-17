USE DW_DataShop;

CREATE TABLE STG_Fact_Ventas.sql (
    FechaVenta DATE,
    CodProducto INT,
    Producto NVARCHAR(200),
    Cantidad INT,
    PrecioVenta DECIMAL(18,2),
    CodCliente INT,
    Cliente NVARCHAR(200),
    CodTienda INT,
    Tienda NVARCHAR(200)
);
