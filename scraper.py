import requests
import time
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from config import settings
from models import BonalyzeOffer, MarktguruOffer, OfferImage

from supabase import create_client, Client

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Scraper:
    def __init__(self, discovered_headers: Optional[Dict[str, str]] = None):
        self.session = requests.Session()
        
        # Base headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Origin": "https://www.marktguru.de",
            "Referer": "https://www.marktguru.de/"
        }
        
        # Merge discovered headers if available
        if discovered_headers:
            logger.info("Scraper: Applying discovered headers from Sentinel.")
            headers.update(discovered_headers)
        else:
            logger.warning("Scraper: No discovered headers provided, using static config (fallback).")
            headers.update({
                "x-apikey": settings.MARKETGURU_API_KEY,
                "x-clientkey": settings.MARKETGURU_CLIENT_KEY,
            })
            
        self.session.headers.update(headers)
        
        # Initialize Supabase client for retailer configs
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        self.retailer_mapping: Dict[str, str] = {}

    def load_retailer_configs(self):
        """Load retailer mapping from Supabase."""
        try:
            logger.info("Scraper: Loading retailer configurations from Supabase...")
            response = self.supabase.table("retailer_configs").select("retailer_key, retailer_id").eq("is_active", True).execute()
            if response.data:
                self.retailer_mapping = {row["retailer_key"]: row["retailer_id"] for row in response.data}
                logger.info(f"Scraper: Loaded {len(self.retailer_mapping)} retailer configs: {list(self.retailer_mapping.keys())}")
            else:
                logger.warning("Scraper: No active retailer configs found in Supabase.")
        except Exception as e:
            logger.error(f"Scraper: Failed to load retailer configs: {e}")

    @retry(stop=stop_after_attempt(settings.MAX_RETRIES), wait=wait_fixed(settings.RETRY_DELAY), retry=retry_if_exception_type(requests.RequestException))
    def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.get(url, params=params, timeout=settings.DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def fetch_offers(self, retailer_key: str, max_items: Optional[int] = None) -> List[BonalyzeOffer]:
        """Fetch all offers for a given retailer."""
        if not self.retailer_mapping:
            self.load_retailer_configs()

        retailer_id = self.retailer_mapping.get(retailer_key.lower())
        if not retailer_id:
            logger.error(f"Unknown retailer key: {retailer_key}")
            return []

        all_offers: List[BonalyzeOffer] = []
        limit = settings.SCRAPER_BATCH_SIZE
        offset = 0
        total_results = None

        logger.info(f"Fetching offers for {retailer_key} (ID: {retailer_id})...")

        while True:
            # Check max_items
            if max_items and len(all_offers) >= max_items:
                break
            try:
                url = f"https://{settings.API_HOST}/api/v1/offers"
                params = {
                    "retailerIds": retailer_id,
                    "zipCode": settings.ZIP_CODE,
                    "limit": limit,
                    "offset": offset
                }
                
                data = self._make_request(url, params)

                if total_results is None:
                    total_results = data.get("totalResults", 0)
                    logger.info(f"Total results expected: {total_results}")

                results = data.get("results", [])
                if not results:
                    break

                for item in results:
                    parsed = self._parse_offer(item, retailer_key)
                    if parsed:
                        all_offers.append(parsed)

                offset += limit
                logger.info(f"Fetched {len(all_offers)}/{total_results} offers...")

                if offset >= total_results:
                    break
                
                # Rate limiting
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error fetching offers at offset {offset}: {e}")
                break

        return all_offers

    def _parse_offer(self, item: Dict[str, Any], retailer: str) -> Optional[BonalyzeOffer]:
        """Parse a single offer item."""
        try:
            # Pydantic parsing for validation (strict)
            mg_offer = MarktguruOffer(**item)
            
            # Helper to access nested data safely from the model
            product = mg_offer.product
            name = product.name
            description = mg_offer.description or product.description
            
            full_name = name
            if description and description not in [name, ""]:
                full_name = f"{name} {description}"

            price = mg_offer.price
            ref_price = mg_offer.referencePrice
            old_price = mg_offer.oldPrice
            
            regular_price = ref_price if ref_price is not None else (old_price if old_price is not None else price)

            valid_from = None
            valid_to = None
            if mg_offer.validityDates:
                valid_from = mg_offer.validityDates[0].from_
                valid_to = mg_offer.validityDates[0].to

            unit = None
            if mg_offer.unit:
                unit = mg_offer.unit.shortName
            amount = mg_offer.quantity

            image_url = None
            if mg_offer.id:
                image_url = f"https://mg2de.b-cdn.net/api/v1/offers/{mg_offer.id}/images/default/0/medium.webp"

            return BonalyzeOffer(
                retailer=retailer,
                product_name=full_name,
                price=float(price),
                regular_price=float(regular_price),
                unit=unit,
                amount=amount,
                currency="EUR",
                valid_from=valid_from,
                valid_to=valid_to,
                image_url=image_url,
                offer_id=str(mg_offer.id),
                raw_data=item
            )

        except Exception as e:
            logger.warning(f"Error parsing item {item.get('id')}: {e}")
            return None

if __name__ == "__main__":
    # Test run
    s = Scraper()
    offers = s.fetch_offers("kaufland", max_items=1)
    print(f"Total offers retrieved: {len(offers)}")
    if offers:
        print("Sample offer:", offers[0])
