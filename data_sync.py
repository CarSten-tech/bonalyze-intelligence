import logging
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import List, Dict, Any
from postgrest.exceptions import APIError
from datetime import datetime

from config import settings
from models import BonalyzeOffer

logger = logging.getLogger(__name__)

class DataSync:
    def __init__(self):
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
             raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set.")
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type((APIError, Exception)))
    def sync_offers_batch(self, offers: List[BonalyzeOffer]) -> Dict[str, int]:
        """
        Syncs a batch of offers to Supabase.
        Upsert is efficient for batches.
        """
        if not offers:
            return {"inserted": 0, "updated": 0, "failed": 0}

        stats = {"inserted": 0, "updated": 0, "failed": 0}
        
        data_to_upsert = []
        for offer in offers:
            row = offer.model_dump(exclude={"raw_data"})
            row["store"] = row.pop("retailer")
            data_to_upsert.append(row)

        if not data_to_upsert:
             return stats

        try:
            response = self.supabase.table("offers").upsert(data_to_upsert).execute()
            if response.data:
                stats["inserted"] += len(response.data)
            
        except Exception as e:
            logger.error(f"Batch upsert failed: {e}")
            stats["failed"] += len(offers)
            raise e 

        return stats

    def delete_expired_offers(self):
        """
        Deletes offers where valid_to is in the past.
        """
        try:
            now = datetime.now().isoformat()
            response = self.supabase.table("offers").delete().lt("valid_to", now).execute()
            
            count = len(response.data) if response.data else 0
            logger.info(f"Pruned {count} expired offers.")
            
        except Exception as e:
            logger.error(f"Error pruning expired offers: {e}")

    def sync_offer(self, offer: dict):
        """Legacy wrapper"""
        pass
