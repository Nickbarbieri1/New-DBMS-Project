from itertools import combinations
import Connector
import Generate_dataset
import json
import os
import sys
from datetime import datetime
from random import randrange
import pymongo

DB_NAME="ProjectDB"
CONN_STR="mongodb://localhost:27017/"

def get_period_of_day(t):
    t = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")
    hour = t.hour
    
    if 6<= hour <12:
        return "morning"
    elif 12 <= hour < 18:
        return "afternoon"
    elif 18 <= hour < 22:
        return "evening"
    else:
        return "night"

def update_ops(db):
    
    products = ["high-tech","food","clothing","consumable","other"]
    #UPDATE 1: aggiunta campo "period of the day"
    #UPDATE 2: aggiunta campo "kind_product"
    #UPDATE 3: aggiunta campo "security_index"
    for tnx in db.transactions.find():
        period = get_period_of_day(tnx["TX_DATETIME"])
        db.transactions.update_one(
            {"_id": tnx["_id"]},
            {"$set": {"period_of_day": period}}
        )
        
        index = randrange(0,len(products))
        db.transactions.update_one(
            {"_id": tnx["_id"]},
            {"$set": {"product_kind": products[index]}}
        )
        
        sec_val = randrange(1,6)
        db.transactions.update_one(
            {"_id": tnx["_id"]},
            {"$set": {"security_index": sec_val}}
        )
        
    #UPDATE 4: creazione collezione "buying_friends" in base al livello di sicurezza ed alle transazioni effettuate
    pipeline = [
        {
            "$group": {
                "_id": {"user": "$CUSTOMER_ID", "terminal": "$TERMINAL_ID"}, 
                "avgFeeling": {"$avg": "$security_index"},
                "count": {"$sum": 1}
            }
        },
        {"$match": {"count": {"$gte": 3}}}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    
    print("SONO ARRIVATO FINO A QUI!")
    
    by_terminal = {}
    for r in results:
        term = r["_id"]["terminal"]
        by_terminal.setdefault(term, []).append((r["_id"]["user"], r["avgFeeling"]))
        
    pairs = set()
    for term, users in by_terminal.items():
        for (u1, f1), (u2, f2) in combinations(users, 2):
            if abs(f1 - f2) < 1:
                pairs.add(tuple(sorted([u1, u2])))
                
    buying_friends = db["buying_friends"]
    counter = 0
    for u1, u2 in pairs:
        if counter == 10000:
            break
        buying_friends.update_one(
            {"user_1": u1, "user_2": u2},
            {"$setOnInsert": {"user_1": u1, "user_2": u2}},
            upsert=True
        )
        counter+=1
    
   
    
    
    
    

if __name__ == "__main__":
    client = pymongo.MongoClient(CONN_STR)
    
    db = client[DB_NAME]
    
    update_ops(db=db)
    
    print("")
    print("QUERY 1")
    print("")
    
    # QUERY 1
    # Step 1: Crea collezione temporanea con profilo cliente
    pipeline_profile = [
        {
            "$group": {
                "_id": "$CUSTOMER_ID",
                "total_spending": { "$sum": "$TX_AMOUNT" },
                "terminals_used": { "$addToSet": "$TERMINAL_ID" }
            }
        },
        {
            "$lookup": {
                "from": "customers",
                "localField": "_id",
                "foreignField": "CUSTOMER_ID",
                "as": "customer_info"
            }
        },
        { "$unwind": "$customer_info" },
        {
            "$project": {
                "customer_id": "$_id",
                "total_spending": 1,
                "terminals_used": 1
            }
        },
        { "$out": "customer_profiles" }
    ]

    # Esegui la pipeline
    db.transactions.aggregate(pipeline_profile)
    
    
    pipeline_match = [
        {
            "$lookup": { # faccio il match
                "from": "customer_profiles",
                "as": "other_customers",
                "let": {
                    "x_id": "$customer_id",
                    "x_terminals": "$terminals_used",
                    "x_spending": "$total_spending"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$ne": ["$customer_id", "$$x_id"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "customer_id": 1,
                            "total_spending": 1,
                            "terminals_used": 1,
                            "shared_terminals": {
                                "$setIntersection": ["$terminals_used", "$$x_terminals"]
                            },
                            "x_spending": "$$x_spending"
                        }
                    },
                    {
                        "$addFields": {
                            "shared_terminal_count": { "$size": "$shared_terminals" },
                            "spending_diff_ratio": {
                                "$abs": {
                                    "$divide": [
                                        { "$subtract": ["$total_spending", "$x_spending"] },
                                        "$x_spending"
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "shared_terminal_count": { "$gte": 3 },
                            "spending_diff_ratio": { "$lte": 0.10 }
                        }
                    },
                    {
                        "$project": {
                            "x_id": "$$x_id",
                            "x_spending": "$$x_spending",
                            "y_id": "$customer_id",
                            "y_spending": "$total_spending"
                        }
                    }
                ]
            }
        },
        { "$unwind": "$other_customers" },
        {
            "$project": {
                "_id": 0,
                "x_id": "$customer_id",
                "x_spending": "$total_spending",
                "y_id": "$other_customers.y_id",
                "y_spending": "$other_customers.y_spending"
            }
        }
    ]

    # Esecuzione della query
    results = db.customer_profiles.aggregate(pipeline_match)
    # Faccio in modo di cancellare la collezione temporanea una volta finita la query
    db.customer_profiles.drop()

    # Stampa dei primi 100 risultati
    counter = 0
    for r in results:
        if counter < 100:
            print(r)
            counter+=1
        else:
            break
        
        
    print("")
    print("QUERY 2")
    print("")
# QUERY 2 

    # Procedo ad estrarre mese ed anno dalla data attuale
    today = datetime(2025,2,1) # specifico la data che voglio, cosi' da poter gestire in maniera migliore i risultati della query
    current_month = today.month
    current_year = today.year

    # Imposto i valori corretti di mese ed anno prima di effettuare il filtraggio delle transazioni
    if current_month == 1:
        prev_month = 12
        prev_year = current_year - 1
    else:
        prev_month = current_month - 1
        prev_year = current_year
        
    
    #MARK: Togli i commenti dalle due linee sotto per far funzionare la query visto che non ci sono sufficienti transazioni in tutti i mesi del 2025
    #prev_month=1
    #current_month=2
        

    # Pipeline per calcolare la media per terminale del mese precedente
    avg_pipeline = [
        {
            "$addFields": {
                "year": { "$year": { "$toDate": "$TX_DATETIME" } },
                "month": { "$month": { "$toDate": "$TX_DATETIME" } }
            }
        },
        {
            "$match": {
                "year": prev_year,
                "month": prev_month
            }
        },
        {
            "$group": {
                "_id": "$TERMINAL_ID",
                "avg_prev_month": { "$avg": "$TX_AMOUNT" }
            }
        },
        { "$out": "avg_amount_prev_month" }  # Salvo il risultato della pipeline in una collezione temporanea
    ]

    db.transactions.aggregate(avg_pipeline)
    
    fraud_pipeline = [
        {
            "$addFields": {
                "year": { "$year": { "$toDate": "$TX_DATETIME" } },
                "month": { "$month": { "$toDate": "$TX_DATETIME" } }
            }
        },
        {
            "$match": { 
                "year": current_year,
                "month": current_month
            }
        },
        {
            "$lookup": { #faccio il lookup con le transazioni della collezione temporanea
                "from": "avg_amount_prev_month",
                "localField": "TERMINAL_ID",
                "foreignField": "_id",
                "as": "avg_data"
            }
        },
        { "$unwind": "$avg_data" },
        {
            "$addFields": { #aggiungo un campo che rappresenta il valore delle transazioni del mese precedente con gia' sommato il 20%
                "threshold": { "$multiply": ["$avg_data.avg_prev_month", 1.2] }
            }
        },
        {
            "$match": {
                "$expr": { "$gt": ["$TX_AMOUNT", "$threshold"] }
            }
        },
        {
            "$project": {
                "_id": 0,
                "TERMINAL_ID": 1,
                "TX_AMOUNT": 1,
                "TX_DATETIME": 1,
                "avg_prev_month": "$avg_data.avg_prev_month",
                "threshold": 1
            }
        }
    ]

    results = db.transactions.aggregate(fraud_pipeline)
    db.avg_amount_prev_month.drop()
    
    # Stampa dei primi 100 risultati
    counter = 0
    for r in results:
        if counter < 100:
            print(r)
            counter+=1
        else:
            break
    
    
#QUERY 3

    print("")
    print("QUERY 3")
    print("")

    starting_customer_id = input("Inserisci l'identificativo dell'utente interessato: ")

    """
    pipeline = [
        # Step 1: terminali usati da u
        {
            "$match": { "CUSTOMER_ID": starting_customer_id }
        },
        {
            "$group": {
                "_id": None,
                "terminals_u": { "$addToSet": "$TERMINAL_ID" }
            }
        },

        # Step 2: trova u2 che hanno usato gli stessi terminali
        {
            "$lookup": {
                "from": "transactions",
                "let": { "terminals": "$terminals_u" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$in": ["$TERMINAL_ID", "$$terminals"] },
                                    { "$ne": ["$CUSTOMER_ID", starting_customer_id] }
                                ]
                            }
                        }
                    },
                    { "$project": { "_id": 0, "CUSTOMER_ID": 1, "TERMINAL_ID": 1 } }
                ],
                "as": "u2_links"
            }
        },

        # Step 3: ottieni tutti terminali usati dagli u2
        { "$unwind": "$u2_links" },

        {
            "$group": {
                "_id": "$u2_links.CUSTOMER_ID",
                "u2_id": { "$first": "$u2_links.CUSTOMER_ID" },
                "terminals_u2": { "$addToSet": "$u2_links.TERMINAL_ID" }
            }
        },

        # Step 4: trova u3 che condividono terminali con ciascun u2
        {
            "$lookup": {
                "from": "transactions",
                "let": { "terminals": "$terminals_u2", "u2_id": "$u2_id" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$in": ["$TERMINAL_ID", "$$terminals"] },
                                    { "$ne": ["$CUSTOMER_ID", "$$u2_id"] },
                                    { "$ne": ["$CUSTOMER_ID", starting_customer_id] }
                                ]
                            }
                        }
                    },
                    { "$project": { "_id": 0, "CUSTOMER_ID": 1, "TERMINAL_ID": 1 } }
                ],
                "as": "u3_links"
            }
        },
        { "$unwind": "$u3_links" },

        {
            "$group": {
                "_id": "$u3_links.CUSTOMER_ID",
                "u3_id": { "$first": "$u3_links.CUSTOMER_ID" },
                "terminals_u3": { "$addToSet": "$u3_links.TERMINAL_ID" },
                "visited_u2": { "$addToSet": "$u2_id" }
            }
        },

        # Step 5: trova u4 che condividono terminali con ciascun u3
        {
            "$lookup": {
                "from": "transactions",
                "let": {
                    "terminals": "$terminals_u3",
                    "u3_id": "$u3_id",
                    "visited_u2": "$visited_u2"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$in": ["$TERMINAL_ID", "$$terminals"] },
                                    { "$ne": ["$CUSTOMER_ID", "$$u3_id"] },
                                    { "$ne": ["$CUSTOMER_ID", starting_customer_id] },
                                    { "$not": { "$in": ["$CUSTOMER_ID", "$$visited_u2"] } }
                                ]
                            }
                        }
                    },
                    { "$project": { "_id": 0, "CUSTOMER_ID": 1 } }
                ],
                "as": "u4_links"
            }
        },
        { "$unwind": "$u4_links" },

        # Step 6: restituisci i CC3
        {
            "$group": {
                "_id": starting_customer_id,
                "CC3": { "$addToSet": "$u4_links.CUSTOMER_ID" }
            }
        }
    ]

    results = list(db.transactions.aggregate(pipeline))
    """
    
    # 1. Costruzione customer_links (grafo cliente-cliente)
    pipeline_links = [
        {
            '$lookup': {
                'from': 'transactions', 
                'localField': 'TERMINAL_ID', 
                'foreignField': 'TERMINAL_ID', 
                'as': 'customers1'
            }
        }, {
            '$lookup': {
                'from': 'transactions', 
                'localField': 'TERMINAL_ID', 
                'foreignField': 'TERMINAL_ID', 
                'as': 'customers2'
            }
        }, {
            '$unwind': {
                'path': '$customers1', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$unwind': {
                'path': '$customers2', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$project': {
                'TERMINAL_ID': 1, 
                'customers1.CUSTOMER_ID': 1, 
                'customers2.CUSTOMER_ID': 1
            }
        }, {
            '$match': {
                '$expr': {
                    '$ne': [
                        '$customers1.CUSTOMER_ID', '$customers2.CUSTOMER_ID'
                    ]
                }
            }
        }, {
            '$limit': 10000
        }, {
            '$project': {
                'to': '$customers1.CUSTOMER_ID', 
                'from': '$customers2.CUSTOMER_ID', 
                'terminal': '$TERMINAL_ID', 
                '_id': 0
            }
        }, {
            '$merge': {
                'into': 'user_links', 
                'whenMatched': 'replace', 
                'whenNotMatched': 'insert'
            }
        }
    ]

    # Esegui costruzione customer_links
    db.transactions.aggregate(pipeline_links)
    
    
    cc3_pipeline = [
        { "$match": { "from": starting_customer_id } },
        {
            "$graphLookup": {
                "from": "customer_links",
                "startWith": "$to",
                "connectFromField": "to",
                "connectToField": "from",
                "as": "cc3_chain",
                "maxDepth": 2,  # Profondità 3 clienti = 2 passaggi
                "depthField": "depth",
                "restrictSearchWithMatch": { "to": { "$ne": starting_customer_id } }
            }
        },
        {
            "$project": {
            "_id": 0,
            "cc3_users": {
                "$filter": {
                "input": "$cc3_users",
                "as": "user",
                "cond": { "$eq": ["$$user.depth", 1] } # Solo profondità 2 = distanza 3
                }
            }
            }
        }
    ]

    results = list(db.customer_links.aggregate(cc3_pipeline))
    
    print(results)
    
    update_ops(db=db)
    