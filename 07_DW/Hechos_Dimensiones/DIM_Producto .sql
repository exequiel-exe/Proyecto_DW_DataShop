USE DW_DataShop;

CREATE TABLE DIM_Producto (
    
    SK_Producto INT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    CodProducto VARCHAR(100) NOT NULL,
    Descripcion         VARCHAR(300) NULL,
    Categoria           VARCHAR(150) NULL,
    Marca               VARCHAR(150) NULL,
    PrecioCosto         DECIMAL(18,2) NULL,
    PrecioVentaSugerido DECIMAL(18,2) NULL,

    FechaCarga DATETIME NOT NULL DEFAULT (GETDATE())
);
