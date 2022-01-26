import pymongo

class ConnectMongo(object):
    
    def __init__(self):
        myclient = pymongo.MongoClient("mongodb+srv://root:neFdrAvoQTdYl7pV@cluster0.7faf6.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

        self.db = myclient['projeto_ifb']
        self.coleta_dados = self.db['coleta_dados']
        
        self.db.coleta_dados.drop()

    def inserir_registro(self,query):
        self.db.coleta_dados.insert_many(query)