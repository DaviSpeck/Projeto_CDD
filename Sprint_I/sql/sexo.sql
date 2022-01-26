CREATE TABLE IF NOT EXISTS sexo (
        idsexo INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        sexo   VARCHAR(10) NOT NULL,
        sigla  CHAR(1) NOT NULL
        
);

INSERT INTO sexo (sexo,sigla) VALUES ('feminino','f'), ('masculino','m');