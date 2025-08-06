
import pymongo
import requests
import time
from pymongo import MongoClient,  UpdateOne

from listing_fetcher import ListingFetcher

# --- Configuration ---
URL = "https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=UD&idNazione=IT&idContratto=2&idCategoria=1&prezzoMassimo=1200&__lang=it&minLat=46.048872&maxLat=46.07978&minLng=13.189259&maxLng=13.273544&pag=1&paramsCount=5&path=%2Faffitto-case%2Fudine-provincia%2F"
MONGO_URI =  "mongodb+srv://cluster0.7qska.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "udine"
COLLECTION_NAME = "affito"

def compare_and_sync(collection, results):
    """
    Compares fetched results with the collection and syncs the data.
    - Inserts new documents.
    - Removes documents that are no longer in the results.
    - Updates existing documents.
    """
    print("Comparing and synchronizing data...")

    results_with_id = [r for r in results if r.get("_id")]
    if len(results_with_id) != len(results):
        print("Warning: Some results are missing an '_id' and will be skipped.")

    fetched_ids = {r["_id"] for r in results_with_id}
    
    # Find and remove documents that are in the DB but not in the latest fetch
    existing_ids = {doc["_id"] for doc in collection.find({"deleted": {"$exists": False}}, {"_id": 1})}
    to_remove_ids = existing_ids - fetched_ids
    
    if to_remove_ids:
        print(f"Removing {len(to_remove_ids)} old documents.")
        collection.update_many({"_id": {"$in": list(to_remove_ids)}},{"$set": {"deleted": True}})

    # Upsert all documents from the latest fetch
    if results_with_id:
        for r in results_with_id:
            del r["mLastUpdate"]
        operations = [
            UpdateOne({"_id": r["_id"]}, {"$set": r,"$unset":{"deleted":True}}, upsert=True) for r in results_with_id
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


def fetch_data_and_save_to_mongo():
    """
    Fetches listing data from the URL and saves it to a MongoDB collection.
    """
    print(f"Fetching data from URL: {URL}")

    try:
     

        # --- Extract Results ---
        results = ListingFetcher(URL).fetch_all_listings()

        if not results:
            print("No 'result' field found in the response or it is empty.")
            # If there are no results, we should sync, which will clear the collection.
            pass


        # --- Connect to MongoDB ---
        print(f"Connecting to MongoDB database: '{DATABASE_NAME}'...")
        client = MongoClient(MONGO_URI,
                     tls=True,
                     tlsCertificateKeyFile='X509-cert-2864290664025085959.pem',
                     server_api=pymongo.server_api.ServerApi('1'))
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print("MongoDB connection successful.")

        # --- Compare and Sync Data ---
        compare_and_sync(collection, results)

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
