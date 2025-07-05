import requests

class ListingFetcher:
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_all_listings(self):
        """
        Fetches all listings from the paginated API.
        """
        all_results = []
        page = 1
        max_pages = 1

        while page <= max_pages:
            url = f"{self.base_url}&pag={page}"
            print(f"Fetching data from: {url}")

            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                # Update max_pages on the first iteration
                if page == 1:
                    max_pages = data.get("maxPages", 1)
                    print(f"Total pages to fetch: {max_pages}")

                results = data.get("results", [])
                if not results:
                    print(f"No results found on page {page}.")
                    # Stop if a page has no results, even if max_pages is higher
                    break
                
                all_results.extend(results)
                print(f"Fetched {len(results)} results from page {page}.")

                page += 1

            except requests.exceptions.RequestException as e:
                print(f"An error occurred while fetching page {page}: {e}")
                # Stop on error
                break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break
        
        print(f"\nTotal listings fetched: {len(all_results)}")

        
        # --- Add custom _id to each result ---
        for result in all_results:
            # Use the 'id' from the 'realEstate' object as the MongoDB '_id'
            if result.get("realEstate") and result["realEstate"].get("id"):
                result["_id"] = result["realEstate"]["id"]

        return all_results

if __name__ == '__main__':
    # Example usage:
    URL = "https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=UD&idNazione=IT&idContratto=2&idCategoria=1&prezzoMassimo=1200&__lang=it&minLat=46.048872&maxLat=46.07978&minLng=13.189259&maxLng=13.273544&paramsCount=5&path=%2Faffitto-case%2Fudine-provincia%2F"
    
    fetcher = ListingFetcher(URL)
    all_listings = fetcher.fetch_all_listings()
    
    # You can now process all_listings
    # For example, print the ID of the first listing if it exists
    if all_listings:
        first_listing_id = all_listings[0].get("realEstate", {}).get("id", "N/A")
        print(f"\nFirst listing ID: {first_listing_id}")