import os
import datetime
import argparse
import time
import logging
from typing import List, Dict

# Local imports
from scraper import Scraper
from config import settings
from models import BonalyzeOffer

# Configure structured logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Bonalyze")

# Conditionally import these to avoid initialization errors if keys are missing during dry run
try:
    from embedder import Embedder
    from data_sync import DataSync
except ImportError:
    Embedder = None
    DataSync = None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run without DB/API writes")
    args = parser.parse_args()

    logger.info(f"Starting Bonalyze Intelligence... (Dry Run: {args.dry_run})")

    # 1. Initialize Components
    embedder = None
    data_sync = None
    
    if not args.dry_run:
        try:
            logger.info("Initializing Embedder and DataSync...")
            embedder = Embedder()
            data_sync = DataSync()
            
            # Prune expired offers before processing
            logger.info("Pruning expired offers...")
            data_sync.delete_expired_offers()
            
        except Exception as e:
            logger.error(f"Initialization Error: {e}")
            return
    else:
        logger.info("Dry Run: Skipping Embedder/DataSync initialization.")

    # 2. Initialize Scraper
    scraper = Scraper()

    # 3. Define Stores to Scrape
    stores = ["kaufland", "edeka", "aldi_sued", "lidl"] # Updated keys to match config/scraper 
    
    total_stats = {"fetched": 0, "inserted": 0, "updated": 0, "failed": 0, "embedded": 0}

    # 4. Scrape & Process
    for store in stores:
        logger.info(f"--- Processing Store: {store} ---")
        
        try:
            # Fetch offers 
            max_items = 10 if args.dry_run else None
            offers: List[BonalyzeOffer] = scraper.fetch_offers(store, max_items=max_items)
            
            count = len(offers)
            total_stats["fetched"] += count
            logger.info(f"Scraper: Found {count} offers for {store}")

            if count == 0:
                continue

            # Process in batches if necessary, but here we can just process all for the store
            # or chunk if memory is concern. Let's process all for simplicity given typical volumes.
            
            # Extract texts for embedding
            product_names = [o.product_name for o in offers]
            
            # Batch Embeddings
            embeddings_map = {}
            if embedder:
                try:
                    logger.info(f"Generating embeddings for {len(product_names)} items...")
                    embeddings_map = embedder.get_embeddings_batch(product_names)
                    total_stats["embedded"] += len(embeddings_map)
                except Exception as e:
                    logger.error(f"Embedding batch failed: {e}")
            
            # Assign embeddings back to offers
            for offer in offers:
                if offer.product_name in embeddings_map:
                    offer.embedding = embeddings_map[offer.product_name]
                
                # Dry run logging
                if args.dry_run:
                    logger.debug(f"[Dry Run] {offer.product_name}: {offer.price}€")

            if args.dry_run:
                logger.info(f"[Dry Run] Would sync {len(offers)} offers for {store}")
                continue

            # Batch Sync
            if data_sync:
                try:
                    logger.info(f"Syncing {len(offers)} offers to Supabase...")
                    stats = data_sync.sync_offers_batch(offers)
                    
                    total_stats["inserted"] += stats.get("inserted", 0)
                    total_stats["updated"] += stats.get("updated", 0)
                    total_stats["failed"] += stats.get("failed", 0)
                    
                    logger.info(f"Sync Stats for {store}: {stats}")
                except Exception as e:
                    logger.error(f"Sync batch failed for {store}: {e}")
                    total_stats["failed"] += len(offers)

            # Rate limit between stores
            time.sleep(1)

        except Exception as e:
            logger.error(f"Error processing {store}: {e}")

    # Final Summary
    logger.info("="*30)
    logger.info("BONALYZE INTELLIGENCE SUMMARY")
    logger.info("="*30)
    logger.info(f"Fetched:  {total_stats['fetched']}")
    logger.info(f"Inserted: {total_stats['inserted']}") # In batch upsert, this wraps both inserts and updates roughly
    logger.info(f"Updated:  {total_stats['updated']}") # Might be 0 if upsert response doesn't distinguish
    logger.info(f"Failed:   {total_stats['failed']}")
    logger.info(f"Embedded: {total_stats['embedded']}")
    logger.info("="*30)

if __name__ == "__main__":
    main()
