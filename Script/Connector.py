import pymongo
import pandas as pd
import json
import os
import sys

DB_NAME="ProjectDB"
CONN_STR="mongodb://localhost:27017/"

def convert_from_json_to_dict(filepath):
    df = pd.read_json(filepath) #lettura come DataFrame
    records = df.to_dict(orient="records") #conversione in dizionario 
    return records


def delete_datasets():
    client = pymongo.MongoClient(CONN_STR)
    db = client[DB_NAME]
    
    db.drop_collection("customers")
    db.drop_collection("terminals")
    db.drop_collection("transactions")
    
    client.close()
    

def import_datasets(dim_mb):
    client = pymongo.MongoClient(CONN_STR)

    if DB_NAME in client.list_database_names():
        delete_datasets()
    db = client[DB_NAME] #Creazione del database
    clients = db["customers"]
    clients.insert_many(convert_from_json_to_dict("Dataset/customers_table_"+dim_mb+"MB.json"))
    terminals = db["terminals"]
    terminals.insert_many(convert_from_json_to_dict("Dataset/terminals_table_"+dim_mb+"MB.json"))
    transactions = db["transactions"]
    transactions.insert_many(convert_from_json_to_dict("Dataset/transactions_table_"+dim_mb+"MB.json"))
    
    
    #MARK: ripetere quanto e' stato fatto sopra per anche i terminali e le transazioni
    
    

if __name__ == "__main__":
    if len(sys.argv) != 2 or not str(sys.argv[1]).isdigit():
        print("ERRORE: bisogna passare la dimensione, in MB, del dataset da utilizzare!")
        exit(-1)
    import_datasets(dim_mb=sys.argv[1])
    print("Operazione di importazione conclusa correttamente!")