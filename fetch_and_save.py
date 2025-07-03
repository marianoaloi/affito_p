
import pymongo
import requests
from pymongo import MongoClient

# --- Configuration ---
URL = "https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=UD&idNazione=IT&idContratto=2&idCategoria=1&prezzoMassimo=1200&__lang=it&minLat=46.048872&maxLat=46.07978&minLng=13.189259&maxLng=13.273544&pag=1&paramsCount=5&path=%2Faffitto-case%2Fudine-provincia%2F"
MONGO_URI =  "mongodb+srv://cluster0.7qska.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "udine"
COLLECTION_NAME = "affito"

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
            return

        # --- Connect to MongoDB ---
        print(f"Connecting to MongoDB database: '{DATABASE_NAME}'...")
        client = MongoClient(MONGO_URI,
                     tls=True,
                     tlsCertificateKeyFile='X509-cert-2864290664025085959.pem',
                     server_api=pymongo.server_api.ServerApi('1'))
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print("MongoDB connection successful.")

        # --- Save to MongoDB ---
        print(f"Inserting {len(results)} documents into collection: '{COLLECTION_NAME}'...")
        # Clear the collection before inserting new data to avoid duplicates
        collection.delete_many({})
        collection.insert_many(results)
        print("All documents have been successfully saved to MongoDB.")

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
