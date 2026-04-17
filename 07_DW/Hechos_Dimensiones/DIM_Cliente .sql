USE DW_DataShop

CREATE TABLE DIM_Cliente (
    SK_Cliente     INT IDENTITY(1,1) PRIMARY KEY,   -- Clave surrogate

    CodCliente     VARCHAR(100) NOT NULL,           -- Business key
    Nombre         VARCHAR(200) NULL,
    Apellido       VARCHAR(200) NULL,
    Telefono       VARCHAR(50) NULL,
    Email          VARCHAR(200) NULL,
    Direccion      VARCHAR(300) NULL,
    Localidad      VARCHAR(150) NULL,
    Provincia      VARCHAR(150) NULL,
    CP             VARCHAR(50) NULL,
    Sexo           VARCHAR(10) NULL,
    Edad           INT NULL,

    FechaCarga     DATETIME NOT NULL DEFAULT(GETDATE())  
);
