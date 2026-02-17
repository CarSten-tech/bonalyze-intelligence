import requests
import time
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from config import settings
from models import BonalyzeOffer, MarktguruOffer, OfferImage

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Scraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "x-apikey": settings.MARKETGURU_API_KEY,
            "x-clientkey": settings.MARKETGURU_CLIENT_KEY,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Origin": "https://www.marktguru.de",
            "Referer": "https://www.marktguru.de/"
        })

    @retry(stop=stop_after_attempt(settings.MAX_RETRIES), wait=wait_fixed(settings.RETRY_DELAY), retry=retry_if_exception_type(requests.RequestException))
    def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.get(url, params=params, timeout=settings.DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def fetch_offers(self, retailer_key: str, max_items: Optional[int] = None) -> List[BonalyzeOffer]:
        """Fetch all offers for a given retailer."""
        retailer_id = settings.RETAILER_IDS.get(retailer_key.lower())
        if not retailer_id:
            logger.error(f"Unknown retailer key: {retailer_key}")
            return []

        all_offers: List[BonalyzeOffer] = []
        limit = 50
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
            # Pydantic parsing for validation (partial)
            # We use a permissive model to extract what we need
            
            # Basic validation
            if not item.get("id"):
                return None
            
            # Map to BonalyzeOffer
            product = item.get("product", {})
            name = product.get("name")
            description = item.get("description") or product.get("description")
            
            full_name = name
            if description and description not in [name, ""]:
                full_name = f"{name} {description}"

            price = item.get("price")
            ref_price = item.get("referencePrice")
            old_price = item.get("oldPrice")
            
            regular_price = ref_price if ref_price else (old_price if old_price else price)

            valid_from = None
            valid_to = None
            dates = item.get("validityDates", [])
            if dates:
                valid_from = dates[0].get("from")
                valid_to = dates[0].get("to")

            unit_data = item.get("unit", {})
            unit = unit_data.get("shortName")
            amount = item.get("quantity")

            image_url = None
            offer_id = item.get("id")
            if offer_id:
                image_url = f"https://mg2de.b-cdn.net/api/v1/offers/{offer_id}/images/default/0/medium.webp"

            return BonalyzeOffer(
                retailer=retailer,
                product_name=full_name,
                price=float(price) if price is not None else 0.0,
                regular_price=float(regular_price) if regular_price is not None else 0.0,
                unit=unit,
                amount=amount,
                currency="EUR",
                valid_from=valid_from,
                valid_to=valid_to,
                image_url=image_url,
                offer_id=str(offer_id),
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
