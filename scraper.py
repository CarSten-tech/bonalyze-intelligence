import requests
import time
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_KEY = "8Kk+pmbf7TgJ9nVj2cXeA7P5zBGv8iuutVVMRfOfvNE="
CLIENT_KEY = "WU/RH+PMGDi+gkZer3WbMelt6zcYHSTytNB7VpTia90="
API_HOST = "api.marktguru.de"
ZIP_CODE = "41460"

RETAILER_IDS = {
    "kaufland": "126654",
    "aldi_sued": "127153",
    "edeka": "126699",
    "lidl": "126679"
}

class Scraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "x-apikey": API_KEY,
            "x-clientkey": CLIENT_KEY,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Origin": "https://www.marktguru.de",
            "Referer": "https://www.marktguru.de/"
        })

    def fetch_offers(self, retailer_key: str, max_items: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch all offers for a given retailer."""
        retailer_id = RETAILER_IDS.get(retailer_key.lower())
        if not retailer_id:
            logger.error(f"Unknown retailer key: {retailer_key}")
            return []

        all_offers = []
        limit = 50
        offset = 0
        total_results = None

        logger.info(f"Fetching offers for {retailer_key} (ID: {retailer_id})...")

        while True:
            # Check max_items
            if max_items and len(all_offers) >= max_items:
                break
            try:
                url = f"https://{API_HOST}/api/v1/offers"
                params = {
                    "retailerIds": retailer_id,
                    "zipCode": ZIP_CODE,
                    "limit": limit,
                    "offset": offset
                }
                
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

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

    def _parse_offer(self, item: Dict[str, Any], retailer: str) -> Optional[Dict[str, Any]]:
        """Parse a single offer item."""
        try:
            # Basic validation
            if not item.get("id"):
                return None

            product = item.get("product", {})
            name = product.get("name")
            description = item.get("description") or product.get("description")
            
            # Construct a full name
            full_name = name
            if description and description not in [name, ""]:
                full_name = f"{name} {description}"

            price = item.get("price")
            ref_price = item.get("referencePrice")
            old_price = item.get("oldPrice")
            
            # Determine regular price (prioritize reference, then old, then current)
            regular_price = ref_price if ref_price else (old_price if old_price else price)

            # Validity Dates
            valid_from = None
            valid_to = None
            dates = item.get("validityDates", [])
            if dates:
                valid_from = dates[0].get("from")
                valid_to = dates[0].get("to")

            # Unit
            unit_data = item.get("unit", {})
            unit = unit_data.get("shortName")
            amount = item.get("quantity")

            # Image
            image_url = None
            images = item.get("images", {}).get("metadata", [])
            # Marktguru images are constructed differently? 
            # Actually JSON output shows image URL in `images` logic is complex or we construct it.
            # Wait, `api_test_offers.json` shows:
            # "images": { "metadata": [ { "width": 1.0, ... } ] }
            # BUT the homepage.html had: https://mg2de.b-cdn.net/api/v1/offers/21623964/images/default/0/small.webp
            # So pattern is: https://mg2de.b-cdn.net/api/v1/offers/{id}/images/default/0/medium.webp
            
            offer_id = item.get("id")
            if offer_id:
                image_url = f"https://mg2de.b-cdn.net/api/v1/offers/{offer_id}/images/default/0/medium.webp"

            return {
                "retailer": retailer,
                "product_name": full_name,
                "price": float(price) if price is not None else 0.0,
                "regular_price": float(regular_price) if regular_price is not None else 0.0,
                "unit": unit,
                "amount": amount,
                "currency": "EUR",
                "valid_from": valid_from,
                "valid_to": valid_to,
                "image_url": image_url,
                "offer_id": str(offer_id),
                "raw_data": item  # Optional: keep for debugging or embeddings
            }

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
