CREATE TABLE IF NOT EXISTS dados (
        iddados INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
        idcoleta   INTEGER   NOT NULL,
        x INTEGER NOT NULL,
        y INTEGER NOT NULL,
        z INTEGER NOT NULL,
        FOREIGN KEY(idcoleta) REFERENCES coleta(idcoleta)
);