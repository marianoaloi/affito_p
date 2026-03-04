import os
import pymongo
import requests
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# filepath: c:\Users\maria\prj\Python\affito_p\get_alteza.py

# --- Configuration ---
MONGO_URI = "mongodb+srv://cluster0.7qska.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "udine"
COLLECTION_NAME = "affito"
GOOGLE_ELEVATION_API = "https://maps.googleapis.com/maps/api/elevation/json"
GOOGLE_API_KEY = "AIzaSyCNi1mBimPVqCnxRyIUwAcW5I6Ws_4xoGI"
MAX_WORKERS = 10

def get_elevation(latitude, longitude):
    """Fetch elevation data from Google Elevation API."""
    try:
        params = {
            "locations": f"{latitude},{longitude}",
            "key": GOOGLE_API_KEY
        }
        response = requests.get(GOOGLE_ELEVATION_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "OK" and data.get("results"):
            return data["results"][0].get("elevation")
        else:
            print(f"API error for coordinates ({latitude}, {longitude}): {data.get('status')}")
            return None
    except Exception as e:
        print(f"Error fetching elevation for ({latitude}, {longitude}): {e}")
        return None

def update_elevation(collection, doc_id, elevation):
    """Update document with elevation data."""
    try:
        collection.update_one(
            {"_id": doc_id},
            {"$set": {"elevation": elevation, "realEstate.mElevationUpdate": time.time()}}
        )
    except Exception as e:
        print(f"Error updating document {doc_id}: {e}")

def fetch_and_update_elevations():
    """Fetch all non-deleted documents and update with elevation data."""
    try:
        # --- Connect to MongoDB ---
        print("Connecting to MongoDB...")
        client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsCertificateKeyFile=os.path.dirname(__file__) + '/X509-cert-2864290664025085959.pem',
            server_api=pymongo.server_api.ServerApi('1')
        )
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print("MongoDB connection successful.")

        # --- Fetch all non-deleted documents ---
        query = {"deleted": {"$exists": False} , "elevation": {"$exists": False}}
        documents = list(collection.find(query))
        print(f"Found {len(documents)} documents to process.")

        if not documents:
            print("No documents to process.")
            return

        # --- Process elevations in parallel ---
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            
            for doc in  documents :
                for p in doc.get("realEstate", {}).get("properties", []):
                    try:
                        lat = p.get("location", {}).get("latitude")
                        lng = p.get("location", {}).get("longitude")
                        
                        if lat is None or lng is None:
                            print(f"Skipping document {doc['_id']}: missing coordinates")
                            continue
                        
                        future = executor.submit(get_elevation, lat, lng)
                        futures[future] = (doc["_id"], lat, lng)
                    except Exception as e:
                        print(f"Error processing document {doc.get('_id')}: {e}")

            # --- Collect results and update MongoDB ---
            completed = 0
            for future in as_completed(futures):
                doc_id, lat, lng = futures[future]
                try:
                    elevation = future.result()
                    if elevation is not None:
                        update_elevation(collection, doc_id, elevation)
                        print(f"Updated {doc_id} with elevation: {elevation}m")
                        completed += 1
                    else:
                        print(f"Failed to get elevation for {doc_id}")
                except Exception as e:
                    print(f"Error processing result for {doc_id}: {e}")

        print(f"Elevation update complete. {completed}/{len(documents)} documents updated.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'client' in locals() and client:
            client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    fetch_and_update_elevations()
    
    
"""
[
  {
    $match:
      /**
       * query: The query in MQL.
       */
      {
        deleted: {
          $exists: false
        }
      }
  },
  {
    $group:
      /**
       * _id: The id of the group.
       * fieldN: The first field name.
       */
      {
        _id: {
          $first:
            "$realEstate.properties.location.province"
        },
        count: {
          $sum: 1
        },
        less: {
          $min: "$elevation"
        },
        great: {
          $max: "$elevation"
        }
      }
  }
]
"""