import sqlite3


class Connect(object):

    def __init__(self):
        try:
            # conectando...
            self.conn = sqlite3.connect("registro.db")
            self.cursor = self.conn.cursor()
            # print("Banco:", db_name)
            # self.cursor.execute('SELECT SQLITE_VERSION()')
            # self.data = self.cursor.fetchone()
            # print("SQLite version: %s" % self.data)

            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='tipomovimento';")
            self.dado = self.cursor.fetchone()

            if self.dado == None or self.dado == '':

                self.criar_schema('Sprint_I/sql/tipomovimento.sql')

            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sexo';")
            self.dado = self.cursor.fetchone()

            if self.dado == None or self.dado == '':
                self.criar_schema('Sprint_I/sql/sexo.sql')

            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='coleta';")
            self.dado = self.cursor.fetchone()

            if self.dado == None or self.dado == '':
                self.criar_schema('Sprint_I/sql/coleta.sql')

            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dados';")
            self.dado = self.cursor.fetchone()

            if self.dado == None or self.dado == '':
                self.criar_schema('Sprint_I/sql/dados.sql')

        except sqlite3.Error:
            print("Erro ao abrir banco.")
            return False

    def commit_db(self):
        if self.conn:
            self.conn.commit()

    def close_db(self):
        if self.conn:
            self.conn.close()

    def ler_registros(self, query):
        sql = query
        r = self.cursor.execute(sql)
        return r.fetchall()
   
    def ler_registro(self,query):
        sql = query
        r = self.cursor.execute(sql)
        return r.fetchone()
   
    def inserir_registro(self, query):
        try:
            self.cursor.execute(query)
            self.data = self.cursor.fetchone()
            self.commit_db()
        except sqlite3.IntegrityError:
            return False

    def criar_schema(self, schema_name):

        try:
            with open(schema_name, 'rt') as f:
                schema = f.read()
                self.cursor.executescript(schema)
        except sqlite3.Error:
            print("Aviso: A tabela já existe.")
        return False
