import Connector
import Generate_dataset
import json
import os
import sys
import pymongo

DB_NAME="ProjectDB"
CONN_STR="mongodb://localhost:27017/"

if __name__ == "__main__":
    client = pymongo.MongoClient(CONN_STR)
    
    db = client[DB_NAME]
    
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