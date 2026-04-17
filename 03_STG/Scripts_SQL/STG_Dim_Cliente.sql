USE DW_DataShop;

CREATE TABLE STG_Dim_Clientes (
    CodCliente INT NULL,
    RazonSocial NVARCHAR(200) NULL,
    Telefono NVARCHAR(50) NULL,
    Email NVARCHAR(200) NULL,
    Direccion NVARCHAR(200) NULL,
    Localidad NVARCHAR(200) NULL,
    Provincia NVARCHAR(200) NULL,
    CP NVARCHAR(20) NULL
);
