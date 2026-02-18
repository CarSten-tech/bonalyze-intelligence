import requests
import time
import logging
import random
from typing import List, Dict, Optional, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from config import settings
from models import BonalyzeOffer, MarktguruOffer
from normalization import normalize_whitespace

from supabase import create_client, Client

logger = logging.getLogger(__name__)
TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def _is_retryable_request_exception(exc: BaseException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code in TRANSIENT_STATUS_CODES
    return False

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
                self.retailer_mapping = {}
                for row in response.data:
                    key = row["retailer_key"]
                    if key == "aldi_sued":
                        key = "aldi-sued"
                    self.retailer_mapping[key] = row["retailer_id"]
                logger.info(f"Scraper: Loaded {len(self.retailer_mapping)} retailer configs: {list(self.retailer_mapping.keys())}")
            else:
                logger.warning("Scraper: No active retailer configs found in Supabase.")
        except Exception as e:
            logger.error(f"Scraper: Failed to load retailer configs: {e}")

    @retry(
        stop=stop_after_attempt(settings.MAX_RETRIES),
        wait=wait_exponential(multiplier=max(settings.RETRY_DELAY, 1), min=1, max=12),
        retry=retry_if_exception(_is_retryable_request_exception),
        reraise=True,
    )
    def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.session.get(url, params=params, timeout=settings.DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            logger.error(f"HTTP Error for {url}: {status_code} - {e.response.text if e.response is not None else str(e)}")
            raise e

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

        target_count = 435 # Baseline expectation from Website analysis
        logger.info(f"Using Publisher-API for {retailer_key}. Target: ~{target_count} items expected.")

        while True:
            # Check max_items
            if max_items and len(all_offers) >= max_items:
                break
            try:
                url = f"https://{settings.API_HOST}/api/v1/publishers/retailer/{retailer_key}/offers"
                params = {
                    "as": "mobile",
                    "zipCode": settings.ZIP_CODE,
                    "limit": limit,
                    "offset": offset
                }
                
                data = self._make_request(url, params)

                if total_results is None:
                    total_results = data.get("totalResults", 0)
                    logger.info(f"Total results available: {total_results}")

                results = data.get("results", [])
                if not results:
                    break

                parsed_in_page: List[BonalyzeOffer] = []
                for item in results:
                    parsed = self._parse_offer(item, retailer_key)
                    if parsed:
                        parsed_in_page.append(parsed)
                        all_offers.append(parsed)

                if parsed_in_page:
                    with_category = sum(1 for offer in parsed_in_page if offer.category)
                    logger.info(
                        f"Category coverage ({retailer_key}, offset {offset}): "
                        f"{with_category}/{len(parsed_in_page)} offers with category."
                    )

                offset += limit
                logger.info(f"Fetched {len(all_offers)} filtered offers (Raw offset: {offset}/{total_results})...")

                if offset >= total_results:
                    break
                
                # Rate limiting with jitter to reduce bot-like patterns.
                fallback_delay = float(getattr(settings, "RETRY_DELAY", 1))
                delay_min_cfg = float(getattr(settings, "SCRAPER_DELAY_MIN_SEC", fallback_delay))
                delay_max_cfg = float(getattr(settings, "SCRAPER_DELAY_MAX_SEC", fallback_delay))
                min_delay = max(0.0, min(delay_min_cfg, delay_max_cfg))
                max_delay = max(min_delay, max(delay_min_cfg, delay_max_cfg))
                time.sleep(random.uniform(min_delay, max_delay))

            except Exception as e:
                logger.error(f"Error fetching offers at offset {offset}: {e}")
                break
        
        logger.info(f"Publisher-API: Received {len(all_offers)} curated items for {retailer_key}.")
        return all_offers

    @staticmethod
    def _build_source_url(item: Dict[str, Any], retailer: str, offer_id: Any) -> str:
        candidates: List[Optional[str]] = [
            item.get("sourceUrl"),
            item.get("sourceURL"),
            item.get("offerUrl"),
            item.get("offerURL"),
            item.get("url"),
            item.get("landingPageUrl"),
            item.get("deeplink"),
        ]
        product = item.get("product")
        if isinstance(product, dict):
            candidates.append(product.get("url"))

        for candidate in candidates:
            if isinstance(candidate, str):
                value = normalize_whitespace(candidate)
                if value:
                    return value

        retailer_slug = normalize_whitespace(retailer).lower() or "unknown"
        offer_id_text = normalize_whitespace(str(offer_id or ""))
        if offer_id_text:
            return f"https://www.marktguru.de/angebote/{retailer_slug}/{offer_id_text}"
        return f"https://www.marktguru.de/angebote/{retailer_slug}"

    @staticmethod
    def _extract_category(category_raw: Any) -> Optional[str]:
        if isinstance(category_raw, str):
            category = normalize_whitespace(category_raw)
            return category or None
        if isinstance(category_raw, dict):
            for key in ("name", "title", "label", "text", "categoryName"):
                candidate = category_raw.get(key)
                if isinstance(candidate, str):
                    category = normalize_whitespace(candidate)
                    if category:
                        return category
            for key in ("category", "categories", "parent", "node"):
                value = Scraper._extract_category(category_raw.get(key))
                if value:
                    return value
        if isinstance(category_raw, list):
            for item in category_raw:
                value = Scraper._extract_category(item)
                if value:
                    return value
        return None

    def _parse_offer(self, item: Dict[str, Any], retailer: str) -> Optional[BonalyzeOffer]:
        """Parse a single offer item with strict filtering."""
        try:
            # Pydantic parsing for validation (strict)
            mg_offer = MarktguruOffer(**item)
            
            # --- FILTER PIPELINE ---
            
            # 1. Index Check: Must be True
            if not mg_offer.retailer or not mg_offer.retailer.indexOffer:
                return None
                
            # 2. Price Check: Removed as per Enterprise-Prompt (Trust curated endoint)
            # 3. Category Check: Removed as per Enterprise-Prompt (Trust curated endpoint)
                
            # 4. Time Check: Duration <= 14 days
            valid_from = None
            valid_to = None
            
            # Priority: Flat fields, then list
            if mg_offer.validFrom and mg_offer.validTo:
                valid_from = mg_offer.validFrom
                valid_to = mg_offer.validTo
            elif mg_offer.validityDates:
                valid_from = mg_offer.validityDates[0].from_
                valid_to = mg_offer.validityDates[0].to
                
            if valid_from and valid_to:
                duration = (valid_to - valid_from).days
                if duration > 14:
                    return None
            else:
                # If no validity info, we skip it for safety (enterprise rule)
                return None

            # --- PARSING ---
            product = mg_offer.product
            name = normalize_whitespace(product.name)
            description = normalize_whitespace(mg_offer.description or product.description or "")
            
            full_name = name
            if description and description not in [name, ""]:
                full_name = normalize_whitespace(f"{name} {description}")

            price = mg_offer.price
            # Use current price as fallback when oldPrice is missing.
            regular_price = mg_offer.oldPrice if mg_offer.oldPrice is not None else mg_offer.price

            unit = None
            if mg_offer.unit:
                unit = mg_offer.unit.shortName
            amount = mg_offer.quantity
            product_raw = item.get("product") if isinstance(item.get("product"), dict) else {}
            category = None
            category_candidates = [
                mg_offer.category,
                mg_offer.categories,
                item.get("category"),
                item.get("categories"),
                item.get("categoryName"),
                item.get("category_name"),
                item.get("categoryTitle"),
                item.get("categoryPath"),
                product_raw.get("category"),
                product_raw.get("categories"),
                product_raw.get("categoryName"),
                product_raw.get("category_name"),
            ]
            for candidate in category_candidates:
                category = self._extract_category(candidate)
                if category:
                    break

            image_url = None
            if mg_offer.id:
                image_url = f"https://mg2de.b-cdn.net/api/v1/offers/{mg_offer.id}/images/default/0/medium.webp"
            source_url = self._build_source_url(item, retailer, mg_offer.id)

            return BonalyzeOffer(
                retailer=retailer,
                product_name=full_name,
                price=float(price),
                regular_price=float(regular_price),
                unit=unit,
                amount=amount,
                currency="EUR",
                category=category,
                valid_from=valid_from,
                valid_to=valid_to,
                image_url=image_url,
                source_url=source_url,
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
