import pymongo
import pandas as pd
import json
import sys

def convert_from_json_to_dict(filepath):
    df = pd.read_json(filepath) #lettura come DataFrame
    records = df.to_dict(orient="records") #conversione in dizionario 
    return records
    

def import_dataset(connector_str,dim_mb):
    client = pymongo.MongoClient(connector_str)
    db = client["ProjectDB"]
    clients = db["client"]
    clients.insert_many(convert_from_json_to_dict("Dataset/customers_table_"+dim_mb+"MB.json"))
    
    #MARK: ripetere quanto e' stato fatto sopra per anche i terminali e le transazioni
    
    

if __name__ == "__main__":
    if len(sys.argv) != 3 or not str(sys.argv[2]).isdigit():
        print("ERRORE: bisogna passare la stringa di connessione a MongoDB da linea di comando e la dimensione, in MB, del dataset da utilizzare!")
        exit(-1)
    import_dataset(connector_str=sys.argv[1], dim_mb=sys.argv[2])