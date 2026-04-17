USE DW_DataShop;

CREATE TABLE Dim_Tiempo (

    Tiempo_Key smalldatetime PRIMARY KEY,
    Anio int,
    Mes int,
    Mes_Nombre varchar(20),
    Semestre int,
    Trimestre int,
    Semana_Anio int,
    Semana_Nro_Mes int,
    Dia int,
    Dia_Nombre varchar(20),
    Dia_Semana_Nro int

);