from concurrent.futures import ProcessPoolExecutor
from itertools import combinations
import Connector
import pandas as pd
import Generate_dataset
import json
import os
import time
from collections import defaultdict, deque
import sys
from datetime import datetime
from random import randrange
import pymongo
from pandarallel import pandarallel

DB_NAME="ProjectDB"
CONN_STR="mongodb://localhost:27017/"

pandarallel.initialize(progress_bar=False)

def get_cck(user_id, k,db):
        """
        Restituisce il set di utenti a distanza esatta k da user_id (CCk),
        dove esiste un arco u--v se u e v hanno condiviso un terminale.
        I risultati sono restituiti come stringhe degli id.
        """
        # 1) costruisci terminal -> lista utenti
        pipeline = [
            {"$group": {"_id": "$TERMINAL_ID", "users": {"$addToSet": "$CUSTOMER_ID"}}}
        ]
        cursor = db.transactions.aggregate(pipeline)

        # 2) costruisci grafo utente -> set(utenti)
        adj = defaultdict(set)
        for doc in cursor:
            users = doc.get("users", [])
            users_s = [str(u) for u in users]   # normalizziamo a stringhe per evitare mismatch
            for u in users_s:
                # aggiungi tutti gli altri utenti connessi tramite questo terminale
                adj[u].update(v for v in users_s if v != u)

        start = str(user_id)
        if start not in adj:
            print(adj)
            print("Esco qui!")
            return set()  # utente senza connessioni

        # 3) BFS fino alla profondità k
        visited = {start}
        frontier = {start}
        depth = 0

        while depth < k:
            next_frontier = set()
            for u in frontier:
                for neigh in adj.get(u, ()):
                    if neigh not in visited:
                        next_frontier.add(neigh)
            visited.update(next_frontier)
            frontier = next_frontier
            depth += 1
            if not frontier:
                break

        # frontier contiene i nodi a distanza esatta k
        return frontier
    
def process_users(users):
    pairs = set()
    for (u1, f1), (u2, f2) in combinations(users, 2):
        if abs(f1 - f2) < 1:
            pairs.add(tuple(sorted((u1, u2)))) #il fatto di ordinarli impedisce di avere documenti duplicati nella collezione (u1 -> u2 e u2 -> u1)
    return pairs

def find_and_store_pairs_pandarallel(by_terminal, db):
    # trasforma in DataFrame per usare pandarallel
    df = pd.DataFrame(
        [(term, users) for term, users in by_terminal.items()],
        columns=["terminal", "users"]
    )

    # calcolo parallelo dei pairs per ogni terminale
    df["pairs"] = df["users"].parallel_apply(process_users)

    # unione dei risultati
    all_pairs = set().union(*df["pairs"])

    # scrittura in bulk su MongoDB
    buying_friends = db["buying_friends"]
    ops = [
        pymongo.UpdateOne(
            {"user_1": u1, "user_2": u2}, #filtro
            {"$setOnInsert": {"user_1": u1, "user_2": u2}}, #updateCommand
            upsert=True
        )
        for u1, u2 in all_pairs
    ]
    if ops:
        buying_friends.bulk_write(ops, ordered=False,bypass_document_validation=True)

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
    
    start_time=time.time()
    for tnx in db.transactions.find():
        #UPDATE 1: aggiunta campo "period of the day"
        period = get_period_of_day(tnx["TX_DATETIME"])
        
        #UPDATE 2: aggiunta campo "kind_product"
        index = randrange(0,len(products))
        
        #UPDATE 3: aggiunta campo "security_index"
        sec_val = randrange(1,6)
        db.transactions.update_one(
            {"_id": tnx["_id"]},
            {"$set": {"security_index": sec_val,"product_kind": products[index],"period_of_day": period}}
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
    
    
    by_terminal = {}
    for r in results:
        term = r["_id"]["terminal"]
        by_terminal.setdefault(term, []).append((r["_id"]["user"], r["avgFeeling"]))
        
    """
    pairs = set()
    for term, users in by_terminal.items():
        for (u1, f1), (u2, f2) in combinations(users, 2):
            if abs(f1 - f2) < 1:
                pairs.add(tuple(sorted([u1, u2])))
                
    buying_friends = db["buying_friends"]
    
    for u1, u2 in pairs:
        buying_friends.update_one(
            {"user_1": u1, "user_2": u2},
            {"$setOnInsert": {"user_1": u1, "user_2": u2}},
            upsert=True
        )
        """
    find_and_store_pairs_pandarallel(by_terminal=by_terminal,db=db)
    print("Tempo per eseguire tutti gli aggiornamenti: {0:.2}s".format(time.time()-start_time))

if __name__ == "__main__":
    if len(sys.argv) != 2 or not str(sys.argv[1]).isdigit():
        print("Ricontrolla il parametro passato al programma (deve essere uno e deve essere o 0 o 1)!")
        exit(-2)
    client = pymongo.MongoClient(CONN_STR)
    
    db = client[DB_NAME]
    
    
    print("")
    print("QUERY 1")
    print("")
    
    start_q1=time.time()
    
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
    
    print("Time to solve query 1 --> {0:.2}s".format(time.time()-start_q1))

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
    
    start_q2=time.time()

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
    
    print("Time to solve query 2 --> {0:.2}s".format(time.time()-start_q2))
    
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
    
    start_q3=time.time()

    # Esempio d'uso:
    cc3 = get_cck(starting_customer_id, 3,db=db)
    
    print("Time to solve query 3 --> {0:.2}s".format(time.time()-start_q3))
    print("CC3("+starting_customer_id+"): ", cc3)
    
    
    if sys.argv[1] == "1": #ci sono da eseguire gli update
        update_ops(db=db)
        #QUERY 4

        print("\nQUERY 4\n")
        start_q4=time.time()
        
        pipeline = [
            {
                "$group": {
                    "_id": "$period_of_day",
                    "totalTransactions": {"$sum": 1},
                    "fraudulentTransactions": {
                        "$sum": {"$cond": ["$TX_FRAUD", 1, 0]}
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "periodOfDay": "$_id",
                    "totalTransactions": 1,
                    "avgFraudulentTransactions": {
                        "$divide": ["$fraudulentTransactions", "$totalTransactions"]
                    }
                }
            }
        ]

        result = list(db.transactions.aggregate(pipeline))
        
        print("Time to solve query 4 --> {0:.2}s".format(time.time()-start_q4))
        
        for elem in result:
            print(elem)
    