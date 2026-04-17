USE DW_DataShop;

CREATE TABLE STG_Dim_Tienda (
    CodTienda INT,
    Descripcion NVARCHAR(200),
    Direccion NVARCHAR(200),
    Localidad NVARCHAR(200),
    Provincia NVARCHAR(200),
    CP NVARCHAR(20),
    TipoTienda NVARCHAR(100)
);
