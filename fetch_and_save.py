
import pymongo
import requests
from pymongo import MongoClient, ReplaceOne

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
        operations = [
            ReplaceOne({"_id": r["_id"]}, r, upsert=True) for r in results_with_id
        ]
        if operations:
            print(f"Upserting {len(operations)} documents (adding new, updating existing).")
            result = collection.bulk_write(operations)
            print(f"Sync result: {result.upserted_count} documents inserted, {result.modified_count} documents updated.")
    
    print("Synchronization complete.")


def fetch_data_and_save_to_mongo():
    """
    Fetches listing data from the URL and saves it to a MongoDB collection.
    """
    print(f"Fetching data from URL: {URL}")

    try:
        # --- Fetch Data from URL ---
        # Set a User-Agent to mimic a browser and avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(URL, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        print("Data fetched successfully.")

        # --- Extract Results ---
        results = data.get("results", [])
        if not results:
            print("No 'result' field found in the response or it is empty.")
            # If there are no results, we should sync, which will clear the collection.
            pass

        # --- Add custom _id to each result ---
        for result in results:
            # Use the 'id' from the 'realEstate' object as the MongoDB '_id'
            if result.get("realEstate") and result["realEstate"].get("id"):
                result["_id"] = result["realEstate"]["id"]

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
