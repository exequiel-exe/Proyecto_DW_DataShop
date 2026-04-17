USE DW_DataShop;

CREATE TABLE STG_Dim_Productos (
    CodProducto INT,
    Descripcion NVARCHAR(200),
    Categoria NVARCHAR(200),
    Marca NVARCHAR(200),
    PrecioCosto DECIMAL(18,2),
    PrecioVentaSugerido DECIMAL(18,2)
);

