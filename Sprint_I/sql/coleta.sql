CREATE TABLE IF NOT EXISTS coleta (
        idcoleta INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
        idsexo   INTEGER   NOT NULL,
        idtipomovimento INTEGER NOT NULL,
        data DATE NOT NULL,
        hora TIME NOT NULL,
        quantidade INTEGER NOT NULL,
        FOREIGN KEY(idsexo) REFERENCES sexo(idsexo),
        FOREIGN KEY(idtipomovimento) REFERENCES tipomovimento(idtipomovimento)
);