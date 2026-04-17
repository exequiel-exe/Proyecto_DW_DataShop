USE DW_DataShop;

CREATE TABLE DIM_Almacen (

    SK_Almacen INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CodAlmacen INT NOT NULL,
    Nombre_Almacen NVARCHAR(150) NOT NULL,
    Localidad NVARCHAR(100) NULL,
    Provincia NVARCHAR(100) NULL,
    FechaCarga DATETIME DEFAULT GETDATE()

);
