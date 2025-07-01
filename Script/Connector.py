import pymongo
import pandas as pd
import json
import os
import sys

DB_NAME="ProjectDB"

def convert_from_json_to_dict(filepath):
    df = pd.read_json(filepath) #lettura come DataFrame
    records = df.to_dict(orient="records") #conversione in dizionario 
    return records

def delete_datasets(connector_str):
    client = pymongo.MongoClient(connector_str)
    db = client[DB_NAME]
    
    db.drop_collection("client")
    db.drop_collection("terminal")
    db.drop_collection("transaction")
    
    client.close()
    

def import_datasets(connector_str,dim_mb):
    client = pymongo.MongoClient(connector_str)

    if DB_NAME in client.list_database_names():
        delete_datasets(connector_str=connector_str)
    db = client[DB_NAME] #Creazione del database
    clients = db["client"]
    clients.insert_many(convert_from_json_to_dict("Dataset/customers_table_"+dim_mb+"MB.json"))
    terminals = db["terminal"]
    terminals.insert_many(convert_from_json_to_dict("Dataset/terminals_table_"+dim_mb+"MB.json"))
    transactions = db["transaction"]
    transactions.insert_many(convert_from_json_to_dict("Dataset/transactions_table_"+dim_mb+"MB.json"))
    
    
    #MARK: ripetere quanto e' stato fatto sopra per anche i terminali e le transazioni
    
    

if __name__ == "__main__":
    conn_str="mongodb://localhost:27017/"
    if len(sys.argv) != 3 or not str(sys.argv[2]).isdigit():
        print("ERRORE: bisogna passare la stringa di connessione a MongoDB da linea di comando e la dimensione, in MB, del dataset da utilizzare!")
        exit(-1)
    import_datasets(connector_str=sys.argv[1], dim_mb=sys.argv[2])