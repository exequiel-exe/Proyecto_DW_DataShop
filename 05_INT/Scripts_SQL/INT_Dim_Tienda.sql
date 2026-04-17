USE DW_DataShop;

CREATE TABLE INT_Dim_Tienda (
    CodTienda   VARCHAR(100) NOT NULL,
    Descripcion VARCHAR(300) NULL,
    Direccion   VARCHAR(300) NULL,
    Localidad   VARCHAR(150) NULL,
    Provincia   VARCHAR(150) NULL,
    CP          VARCHAR(50)  NULL,
    TipoTienda  VARCHAR(150) NULL,
    FechaCarga  DATETIME     NOT NULL DEFAULT (GETDATE())
);
