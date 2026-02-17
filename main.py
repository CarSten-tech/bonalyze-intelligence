import os
import datetime
import argparse
import time
from dotenv import load_dotenv

# Local imports
from scraper import Scraper
# Conditionally import these to avoid initialization errors if keys are missing during dry run
try:
    from embedder import Embedder
    from data_sync import DataSync
except ImportError:
    Embedder = None
    DataSync = None

load_dotenv()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run without DB/API writes")
    args = parser.parse_args()

    print(f"Bonalyze Intelligence: Starting... (Dry Run: {args.dry_run})")

    # 1. Initialize Components
    embedder = None
    data_sync = None
    
    if not args.dry_run:
        try:
            print("Initializing Embedder and DataSync...")
            embedder = Embedder()
            data_sync = DataSync()
        except Exception as e:
            print(f"Initialization Error: {e}")
            return
    else:
        print("Dry Run: Skipping Embedder/DataSync initialization.")

    # 2. Initialize Scraper
    scraper = Scraper()

    # 3. Define Stores to Scrape
    stores = ["kaufland", "edeka", "aldi-sued", "lidl"] 
    
    # 4. Scrape & Process
    for store in stores:
        print(f"\n--- Processing Store: {store} ---")
        
        try:
            # Fetch offers (pagination handles limit)
            # For dry-run, we might want to limit the number of items scraped?
            # efficient-scraping: The `fetch_offers` method already has a loop. 
            # We can pass `max_items` if we want.
            
            max_items = 10 if args.dry_run else None
            offers = scraper.fetch_offers(store, max_items=max_items)
            print(f"Scraper: Found {len(offers)} offers for {store}")

        except Exception as e:
            print(f"Error scraping {store}: {e}")
            offers = []

        if not offers:
             print(f"No offers found for {store}. Skipping.")
             continue

        print(f"Processing {len(offers)} offers...")
        for i, offer in enumerate(offers):
            try:
                # Add metadata
                offer["scraped_at"] = datetime.datetime.now().isoformat()

                if args.dry_run:
                    print(f"[Dry Run] would process: {offer['product_name']} - {offer['price']}€")
                    continue

                # Generate Embedding
                print(f"Generating embedding for: {offer['product_name']}")
                if embedder:
                    # Retry logic for embedding?
                    try:
                        embedding = embedder.get_embedding(offer["product_name"])
                        offer["embedding"] = embedding
                    except Exception as e:
                        print(f"Embedding failed for {offer['product_name']}: {e}")
                        continue # Skip upsert if embedding fails? Or upsert without? 
                        # Usually we want embedding, so skip.
                
                # Sync to Supabase
                if data_sync:
                    data_sync.sync_offer(offer)
                
                # Rate limit for embedding/sync
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error processing offer {offer.get('product_name', 'UNKNOWN')}: {e}")

    print("\nBonalyze Intelligence: Run Complete.")

if __name__ == "__main__":
    main()
