import logging
import base64
import json
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from typing import List, Dict, Any
from postgrest.exceptions import APIError
from datetime import datetime

from config import settings
from models import BonalyzeOffer
from normalization import slugify, normalize_whitespace

logger = logging.getLogger(__name__)
TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def _is_retryable_sync_exception(exc: BaseException) -> bool:
    if isinstance(exc, APIError):
        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int):
            return status_code in TRANSIENT_STATUS_CODES or status_code >= 500
        return False
    return isinstance(exc, (TimeoutError, ConnectionError, OSError))

class DataSync:
    def __init__(self):
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
             raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set.")
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        role = self._extract_jwt_role(settings.SUPABASE_KEY)
        if role and role != "service_role":
            logger.warning(
                f"DataSync: SUPABASE_KEY role is '{role}'. For reliable upsert/delete with RLS, use the service_role key."
            )

    @staticmethod
    def _extract_jwt_role(token: str) -> str | None:
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            payload = parts[1] + "=" * (-len(parts[1]) % 4)
            decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
            obj = json.loads(decoded.decode("utf-8"))
            role = obj.get("role")
            return role if isinstance(role, str) else None
        except Exception:
            return None

    @staticmethod
    def _build_offer_row(offer: BonalyzeOffer) -> Dict[str, Any]:
        row = offer.model_dump(
            mode="json",
            include={
                "product_name",
                "price",
                "regular_price",
                "retailer",
                "image_url",
                "source_url",
                "valid_from",
                "valid_to",
                "scraped_at",
                "embedding",
                "offer_id",
                "currency",
            },
        )
        row["product_name"] = normalize_whitespace(row.get("product_name", ""))
        row["store"] = row.pop("retailer")
        row["original_price"] = row.pop("regular_price", None)
        row["valid_until"] = row.pop("valid_to", None)
        slug = slugify(row.get("product_name", ""))
        row["product_slug"] = slug or f"offer-{row['offer_id']}"
        source_url = normalize_whitespace(row.get("source_url", ""))
        if not source_url:
            store_slug = normalize_whitespace(row.get("store", "")).lower() or "unknown"
            offer_id_text = normalize_whitespace(str(row.get("offer_id", "")))
            if offer_id_text:
                source_url = f"https://www.marktguru.de/angebote/{store_slug}/{offer_id_text}"
            else:
                source_url = f"https://www.marktguru.de/angebote/{store_slug}"
        row["source_url"] = source_url
        return row

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(_is_retryable_sync_exception),
        reraise=True,
    )
    def sync_offers_batch(self, offers: List[BonalyzeOffer]) -> Dict[str, int]:
        """
        Syncs a batch of offers to Supabase.
        Upsert is efficient for batches.
        """
        if not offers:
            return {"inserted": 0, "updated": 0, "failed": 0}

        stats = {"inserted": 0, "updated": 0, "failed": 0}

        offers_to_sync = [o for o in offers if o.embedding and len(o.embedding) == 768]
        skipped_offers = len(offers) - len(offers_to_sync)
        if skipped_offers:
            logger.warning(f"DataSync: Skipping {skipped_offers} offers without valid 768-dim embedding.")
            stats["failed"] += skipped_offers

        if not offers_to_sync:
            return stats
        
        data_to_upsert = []
        for offer in offers_to_sync:
            data_to_upsert.append(self._build_offer_row(offer))

        if not data_to_upsert:
             return stats

        try:
            response = self.supabase.table("offers").upsert(data_to_upsert).execute()
            if response.data:
                stats["inserted"] += len(response.data)
            
        except Exception as e:
            logger.error(f"Batch upsert failed: {e}")
            stats["failed"] += len(offers_to_sync)
            raise e 

        return stats

    def prune_stale_offers(self, run_timestamp: datetime, retailer: str) -> int:
        """
        Deletes offers for a specific retailer that were NOT scraped in the current run.
        This is the 'Sweep' phase of Mark-and-Sweep.
        """
        try:
            timestamp_str = run_timestamp.isoformat()
            logger.info(f"DataSync: Pruning stale offers for {retailer} (not seen since {timestamp_str})...")
            
            # Delete where retailer matches AND scraped_at < run_timestamp
            response = self.supabase.table("offers").delete().eq("store", retailer).lt("scraped_at", timestamp_str).execute()
            
            count = len(response.data) if response.data else 0
            logger.info(f"DataSync: Pruned {count} stale offers for {retailer}.")
            return count
            
        except Exception as e:
            logger.error(f"DataSync: Error pruning stale offers for {retailer}: {e}")
            return 0

    def get_total_count(self) -> int:
        """
        Returns the total number of offers in the database.
        """
        try:
            # count='exact', head=True -> returns count without fetching data
            response = self.supabase.table("offers").select("*", count="exact", head=True).execute()
            return response.count if response.count is not None else 0
        except Exception as e:
            logger.error(f"DataSync: Error getting total count: {e}")
            return 0

    def delete_expired_offers(self):
        """
        Deletes offers where valid_until is in the past.
        """
        try:
            now = datetime.now().isoformat()
            response = self.supabase.table("offers").delete().lt("valid_until", now).execute()
            
            count = len(response.data) if response.data else 0
            logger.info(f"DataSync: Pruned {count} expired offers (global).")
            
        except Exception as e:
            logger.error(f"DataSync: Error pruning expired offers: {e}")

    def sync_offer(self, offer: dict):
        """Legacy wrapper"""
        raise NotImplementedError("sync_offer is deprecated. Use sync_offers_batch instead.")
