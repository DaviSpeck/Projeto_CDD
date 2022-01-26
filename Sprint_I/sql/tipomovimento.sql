CREATE TABLE IF NOT EXISTS tipomovimento (
        idtipomovimento INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
        descricao       VARCHAR(30) NOT NULL,
        model           BOOLEAN NOT NULL
);