USE DW_DataShop;

CREATE TABLE DIM_EstadoPedido (

    SK_EstadoPedido INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- La llave subrogada
    CodEstado INT NOT NULL,                                -- El código original
    Descripcion_Estado NVARCHAR(100) NOT NULL,
    FechaCarga DATETIME DEFAULT GETDATE()

);