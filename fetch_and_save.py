
from enum import Enum
import os
import pymongo
import requests
import time
from pymongo import MongoClient,  UpdateOne

from listing_fetcher import ListingFetcher

class ImmobiliareType(Enum):
    AFFITTO = 'a'
    COMPRA = 'c'

# --- Configuration ---
URL_COMPRA_UD = "https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=UD&idComune=6437&idNazione=IT&idContratto=1&idCategoria=1&prezzoMassimo=200000&__lang=it&minLat=46.012105&maxLat=46.106327&minLng=13.143768&maxLng=13.347702&pag=1&paramsCount=5&path=%2Fvendita-case%2Fudine%2F"
URL_TS="https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=TS&idComune=6307&idNazione=IT&prezzoMassimo=1200&__lang=it&idContratto=2&idCategoria=1&pag=1&paramsCount=0&path=%2Faffitto-case%2Ftrieste%2F"
URL = "https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=UD&idNazione=IT&idContratto=2&idCategoria=1&prezzoMassimo=1200&__lang=it&minLat=46.048872&maxLat=46.07978&minLng=13.189259&maxLng=13.273544&pag=1&paramsCount=5&path=%2Faffitto-case%2Fudine-provincia%2F"
MONGO_URI =  "mongodb+srv://cluster0.7qska.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "udine"
COLLECTION_AFFITO = "affito"
COLLECTION_COMPRA = "compra"
COLLECTION_PRIMARYFEATURES = "primaryFeatures"

def compare_and_sync(collection, results, type: ImmobiliareType ):
    """
    Compares fetched results with the collection and syncs the data.
    - Inserts new documents.
    - Removes documents that are no longer in the results.
    - Updates existing documents.
    """
    print("Comparing and synchronizing data... ",len(results), " items fetched. Type:", type.value)
    
    results = [ {**r, "type": type.value} for r in results]

    results_with_id = [r for r in results if r.get("_id")]
    if len(results_with_id) != len(results):
        print("Warning: Some results are missing an '_id' and will be skipped.")

    fetched_ids = {r["_id"] for r in results_with_id}
    
    # Find and remove documents that are in the DB but not in the latest fetch
    existing_ids = {doc["_id"] for doc in collection.find({ "type": type.value, "deleted": {"$exists": False}}, {"_id": 1})}
    to_remove_ids = existing_ids - fetched_ids
    
    if to_remove_ids:
        print(f"Removing {len(to_remove_ids)} old documents.")
        collection.update_many({"_id": {"$in": list(to_remove_ids)}},{"$set": {"deleted": True}})

    
    items = collection.find({ "type": type.value, "deleted":{"$exists":False}})
    items = {item["realEstate"]["id"]:item for item in items}

    results_with_id = [ r for r in results_with_id 
                        if 
                            not (
                                r["realEstate"]["id"] in items 
                                and 
                                r["realEstate"] == items[r["realEstate"]["id"]]["realEstate"]
                                )
                    ]

    # Upsert all documents from the latest fetch
    if results_with_id:
        for r in results_with_id:
            del r["mLastUpdate"]
            r["mLastImmobiliareUpdate"] = time.time()
        operations = [
            UpdateOne(
                {"_id": r["_id"]}, 
                {"$set": {"realEstate":r["realEstate"] , "type": type.value},"$unset":{"deleted":True}}, upsert=True
            ) for r in results_with_id
        ]
        if operations:
            print(f"Upserting {len(operations)} documents (adding new, updating existing).")
            result = collection.bulk_write(operations)
            print(f"Sync result: {result.upserted_count} documents inserted, {result.modified_count} documents updated.")

        
        operations = [
            UpdateOne({"_id": r}, {"$set": {"mCreateDate":time.time()}}) for r in result.upserted_ids.values()
        ]
        
        if operations:
            result = collection.bulk_write(operations)
           
    print("Synchronization complete.")

def auto_choice(db, type):
    """
    Automatically selects listings based on predefined criteria.
    """
    print("Running auto-choice selection...")
    ids = [int(doc["_id"]) for doc in db[COLLECTION_PRIMARYFEATURES].find({"Accesso_per_disabili":0, "type": type},{"_id":1})]
    if not ids:
        return
    result = db[COLLECTION_AFFITO].update_many({
                        "deleted" : {"$exists":False},
                        "stateMaloi":{"$exists":False},
                        "_id":{"$in":ids}
        }
                                             ,                                
                                             {"$set":{"stateMaloi":0}})
    print(f"Auto-choice selection complete. {result.modified_count} documents updated.")
    
add_type = lambda r,t: {**r, "type":t}

def fetch_data_and_save_to_mongo():
    """
    Fetches listing data from the URL and saves it to a MongoDB collection.
    """

    try:
     

        # --- Extract Results ---
        
        print(f"Fetching data from URL: {URL}")
        results = ListingFetcher(URL).fetch_all_listings()

        
        # print(f"Fetching data from URL: {URL_TS}")
        # results += ListingFetcher(URL_TS).fetch_all_listings()

        if not results:
            print("No 'result' field found in the response or it is empty.")
            # If there are no results, we should sync, which will clear the collection.
            pass


        # --- Connect to MongoDB ---
        print(f"Connecting to MongoDB database: '{DATABASE_NAME}'...")
        client = MongoClient(MONGO_URI,
                     tls=True,
                     tlsCertificateKeyFile= os.path.dirname(__file__) + '/X509-cert-2864290664025085959.pem',
                     server_api=pymongo.server_api.ServerApi('1'))
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_AFFITO]
        print("MongoDB connection successful.")

        # --- Compare and Sync Data ---
        compare_and_sync(collection, results , ImmobiliareType.AFFITTO)
        auto_choice(db, ImmobiliareType.AFFITTO.value)
        
        

        print(f"Fetching data from URL: {URL_COMPRA_UD}")
        results = ListingFetcher(URL_COMPRA_UD).fetch_all_listings()
        
        # --- Define type ---

        # --- Compra collection ---
        compare_and_sync(db[COLLECTION_AFFITO], results , ImmobiliareType.COMPRA)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # --- Close the connection ---
        if 'client' in locals() and client:
            client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    fetch_data_and_save_to_mongo()
