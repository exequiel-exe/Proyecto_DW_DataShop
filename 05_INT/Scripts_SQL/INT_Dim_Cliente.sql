USE DW_DataShop;

CREATE TABLE INT_Dim_Cliente (
    CodCliente        INT              NOT NULL,
    NombreCompleto   VARCHAR(200)     NULL,
    Telefono         VARCHAR(50)      NULL,
    Email            VARCHAR(200)     NULL,
    Direccion        VARCHAR(200)     NULL,
    Localidad        VARCHAR(100)     NULL,
    Provincia        VARCHAR(100)     NULL,
    CP               VARCHAR(20)      NULL,
    Sexo             CHAR(1)          NULL,
    Edad             INT              NULL,
    FechaCarga       DATETIME         NOT NULL DEFAULT (GETDATE())
);
