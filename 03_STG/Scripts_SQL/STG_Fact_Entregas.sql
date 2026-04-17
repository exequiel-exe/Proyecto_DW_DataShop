USE DW_DataShop;

CREATE TABLE STG_Fact_Entregas (
    CodEntrega INT NULL,
    CodVenta INT NULL,
    CodProveedor INT NULL,
    Proveedor NVARCHAR(200) NULL,
    CodAlmacen INT NULL,
    Almacen NVARCHAR(200) NULL,
    CodEstado INT NULL,
    Estado NVARCHAR(100) NULL,
    Fecha_Envio DATE NULL,
    Fecha_Entrega DATE NULL
);

