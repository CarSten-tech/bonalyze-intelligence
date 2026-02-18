import asyncio
import datetime
import argparse
import logging
from typing import List, Dict

# Local imports
from scraper import Scraper
from sentinel import Sentinel
from config import settings
from models import BonalyzeOffer
from embedder import Embedder
from data_sync import DataSync
from run_policy import evaluate_run_failure_reason
from runtime_utils import parse_allowed_stores

# Configure structured logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Bonalyze")

async def main_async():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run without DB/API writes")
    parser.add_argument(
        "--allow-partial-success",
        action="store_true",
        help="Do not fail process if at least one retailer fails.",
    )
    parser.add_argument(
        "--max-failure-rate",
        type=float,
        default=settings.MAX_FAILURE_RATE,
        help="Maximum tolerated failed/fetched ratio before failing the run.",
    )
    args = parser.parse_args()

    run_start_time = datetime.datetime.now()
    logger.info(f"Starting Bonalyze Intelligence (Enterprise Run)... (Start: {run_start_time.isoformat()}, Dry Run: {args.dry_run})")

    # 1. Discovery Phase (Sentinel)
    discovered_headers = {}
    try:
        logger.info("Discovery Phase: Launching Sentinel to capture dynamic headers...")
        sentinel = Sentinel(headless=True)
        discovered_headers = await asyncio.wait_for(sentinel.extract_headers(), timeout=120)
        logger.info(f"Discovery Phase: Captured {len(discovered_headers)} headers.")
    except Exception as e:
        logger.error(f"Discovery Phase Failed: {e}. Falling back to static configuration.")

    # 2. Initialization Phase
    embedder = None
    data_sync = None
    
    if not args.dry_run:
        try:
            logger.info("Initialization Phase: Setting up Embedder and DataSync...")
            embedder = Embedder()
            data_sync = DataSync()
            
            # Prune globally expired offers (pre-run sweep)
            logger.info("Initialization Phase: Pruning globally expired offers...")
            data_sync.delete_expired_offers()
        except Exception as e:
            logger.error(f"Initialization Phase Error: {e}")
            return
    else:
        logger.info("Dry Run: Skipping secondary component initialization.")

    # Initialize Scraper with discovered headers
    scraper = Scraper(discovered_headers=discovered_headers)
    scraper.load_retailer_configs()
    
    # 3. Execution Phase
    logger.info(
        f"Enterprise Sync: Using configured embedding model '{settings.GEMINI_EMBEDDING_MODEL}' "
        f"(api={settings.GEMINI_API_VERSION}, target_dim=768)."
    )
    allowed_stores = parse_allowed_stores(settings.ALLOWED_STORES)
    stores = [s for s in scraper.retailer_mapping.keys() if s in allowed_stores] if scraper.retailer_mapping else allowed_stores
    total_stats: Dict[str, int | float] = {
        "fetched": 0,
        "inserted": 0,
        "failed": 0,
        "embedded": 0,
        "pruned": 0,
        "store_errors": 0,
    }

    for store in stores:
        logger.info(f"--- Processing Retailer: {store} ---")
        store_had_error = False
        
        try:
            # Mark: Fetch offers
            max_items = 10 if args.dry_run else None
            offers: List[BonalyzeOffer] = scraper.fetch_offers(store, max_items=max_items)
            
            count = len(offers)
            total_stats["fetched"] += count
            logger.info(f"Execution Phase: Found {count} active offers for {store}.")

            if count == 0:
                # Still prune if 0 results (maybe they are all gone?)
                # But usually safer to only prune if we actually got a successful response
                continue

            # Embedding (Batch)
            if embedder:
                product_names = [o.product_name for o in offers]
                try:
                    logger.info(f"Embedding Phase: Generating embeddings for {len(product_names)} items...")
                    embeddings_map = embedder.get_embeddings_batch(product_names)
                    
                    # Assign to offers
                    for o in offers:
                        o.embedding = embeddings_map.get(o.product_name)

                    valid_offer_count_before = len(offers)
                    offers = [o for o in offers if o.embedding and len(o.embedding) == 768]
                    dropped_offers = valid_offer_count_before - len(offers)
                    if dropped_offers:
                        logger.warning(f"Embedding Phase: Dropped {dropped_offers} offers without valid 768-dim embeddings.")
                        total_stats["failed"] += dropped_offers
                    if not offers:
                        logger.warning(f"Embedding Phase: No valid embeddings for {store}. Skipping sync/prune for safety.")
                        continue
                    
                    total_stats["embedded"] += len(embeddings_map)
                except Exception as e:
                    logger.error(f"Embedding Phase Failed for {store}: {e}")
                    total_stats["failed"] += len(offers)
                    total_stats["store_errors"] += 1
                    continue

            # Sync to Supabase
            if not args.dry_run and data_sync:
                try:
                    # Enforce consistent timestamp for Mark-and-Sweep
                    # Assign the run_start_time to all offers in this batch
                    for o in offers:
                        o.scraped_at = run_start_time

                    logger.info(f"Sync Phase: Upserting {len(offers)} offers for {store}...")
                    sync_stats = data_sync.sync_offers_batch(offers)
                    total_stats["inserted"] += sync_stats.get("inserted", 0)
                    total_stats["failed"] += sync_stats.get("failed", 0)
                    
                    # Sweep: Mark-and-Sweep Pruning
                    # Remove offers for this retailer that weren't in this successful run
                    pruned_count = data_sync.prune_stale_offers(run_start_time, store)
                    total_stats["pruned"] += pruned_count
                    
                    # Observability: Log total DB count
                    total_db_count = data_sync.get_total_count()
                    logger.info(f"Observability: Current DB Count: {total_db_count} offers")
                    
                except Exception as e:
                    logger.error(f"Sync/Prune Phase Failed for {store}: {e}")
                    total_stats["failed"] += len(offers)
                    total_stats["store_errors"] += 1
                    store_had_error = True
            else:
                 logger.debug(f"[Dry Run] Skipping DB sync for {store}")

            # Politeness
            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Processing Error for {store}: {e}")
            total_stats["store_errors"] += 1
            store_had_error = True

        if not store_had_error:
            logger.info(f"Retailer {store} processed successfully.")

    # 4. Summary Phase
    run_end_time = datetime.datetime.now()
    duration = (run_end_time - run_start_time).total_seconds()
    fetched = int(total_stats["fetched"])
    failed = int(total_stats["failed"])
    failure_rate = (failed / fetched) if fetched else 1.0
    total_stats["failure_rate"] = failure_rate
    
    logger.info("="*50)
    logger.info("FINAL ENTERPRISE RUN SUMMARY")
    logger.info("="*50)
    logger.info(f"Duration:  {duration:.2f} seconds")
    logger.info(f"Fetched:   {total_stats['fetched']}")
    logger.info(f"Upserted:  {total_stats['inserted']} (New/Updated)")
    logger.info(f"Pruned:    {total_stats['pruned']} (Stale/Expired)")
    logger.info(f"Embedded:  {total_stats['embedded']}")
    logger.info(f"Failed:    {total_stats['failed']}")
    logger.info(f"StoreErrors: {total_stats['store_errors']}")
    logger.info(f"FailureRate: {failure_rate:.2%}")
    logger.info("="*50)

    allow_partial_success = args.allow_partial_success or not settings.FAIL_ON_PARTIAL_SYNC
    failure_reason = evaluate_run_failure_reason(
        total_stats,
        dry_run=args.dry_run,
        allow_partial_success=allow_partial_success,
        max_failure_rate=max(args.max_failure_rate, 0.0),
    )
    if failure_reason:
        logger.error(f"Run health check failed: {failure_reason}")
        raise SystemExit(2)
    logger.info("Sync erfolgreich beendet. Alle Batches verarbeitet.")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
