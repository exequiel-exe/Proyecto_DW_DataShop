USE DW_DataShop;

 CREATE TABLE INT_Dim_Producto (
    CodProducto            VARCHAR(100) NOT NULL,
    Descripcion            VARCHAR(300) NULL,
    Categoria              VARCHAR(150) NULL,
    Marca                  VARCHAR(150) NULL,
    PrecioCosto            VARCHAR(50)  NULL,
    PrecioVentaSugerido    VARCHAR(50)  NULL,
    FechaCarga             DATETIME     NOT NULL DEFAULT (GETDATE())
);
