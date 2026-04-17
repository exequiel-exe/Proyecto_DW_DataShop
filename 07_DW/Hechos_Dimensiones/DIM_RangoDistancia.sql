USE DW_DataShop;

CREATE TABLE DIM_RangoDistancia (

    SK_RangoDistancia INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CodRango INT NOT NULL,
    TipoDistancia NVARCHAR(100) NOT NULL,
    Km_Desde INT NOT NULL,
    Km_Hasta INT NOT NULL,
    FechaCarga DATETIME DEFAULT GETDATE()
);
