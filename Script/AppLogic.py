import Connector
import Generate_dataset
import json
import os
import sys
from datetime import datetime
import pymongo

DB_NAME="ProjectDB"
CONN_STR="mongodb://localhost:27017/"

if __name__ == "__main__":
    client = pymongo.MongoClient(CONN_STR)
    
    db = client[DB_NAME]
    
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
    today = datetime.today()
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
    