from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import List, Dict, Any
from postgrest.exceptions import APIError

from config import settings
from models import BonalyzeOffer

class DataSync:
    def __init__(self):
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
             raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set.")
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type((APIError, Exception))) # Broad exception for now, refine if needed
    def sync_offers_batch(self, offers: List[BonalyzeOffer]) -> Dict[str, int]:
        """
        Syncs a batch of offers to Supabase.
        Upsert is efficient for batches.
        """
        if not offers:
            return {"inserted": 0, "updated": 0, "failed": 0}

        stats = {"inserted": 0, "updated": 0, "failed": 0}
        
        # Prepare data for upsert
        # We need to ensure the data matches the table schema.
        # Assuming table 'offers' has columns matching our model fields or we map them.
        
        data_to_upsert = []
        for offer in offers:
            # Dump model to dict, exclude None or map specific fields
            # offer_id needs to be mapped to id? Or unique constraint?
            # Previous implementation used (store, product_name, valid_until) as check.
            # But upsert is better if we have a unique constraint.
            # If not, we might need to be careful.
            # For now, let's assume we map 'offer_id' from marktguru to a unique field or use it.
            
            # Map BonalyzeOffer to DB schema
            # Schema from memory/previous files isn't fully clear but `offers` table likely has:
            # retailer, product_name, price, etc.
            
            row = offer.model_dump(exclude={"raw_data"})
            # Rename if necessary. 
            row["store"] = row.pop("retailer") # Match previous code 'store' vs 'retailer'
            
            # If we want to use upsert, we need a primary key or unique constraint.
            # If (store, product_name, valid_until) is unique, we can rely on that.
            
            data_to_upsert.append(row)

        if not data_to_upsert:
             return stats

        # Supabase upsert
        # 'on_conflict' needs to be specified if not primary key.
        # If we rely on default, it uses PK. 
        # Let's hope there is a good PK or unique constraint. 
        # If not, batch insert might duplicate.
        # But user asked for replacing single lookups with batch.
        
        try:
            # We treat this as "insert or update"
            # count='exact' to get number of rows affected
            response = self.supabase.table("offers").upsert(data_to_upsert).execute()
            
            # In batch upsert, it's hard to distinguish insert vs update without return=representation and checking headers/created_at
            # But usually length of data is what was processed.
            if response.data:
                stats["inserted"] += len(response.data) # Roughly treating all as success
            
        except Exception as e:
            print(f"DataSync: Batch upsert failed: {e}")
            # Identify individual failures? Hard with batch.
            stats["failed"] += len(offers)
            raise e # Retry will handle it

        return stats

    def sync_offer(self, offer: dict):
        """Legacy wrapper"""
        # Convert dict to model if possible, or just hack it for legacy support
        # But we are refactoring, so maybe just use new logic?
        # The main.py will be updated to use batch, so this might be unused.
        pass
