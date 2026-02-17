import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

class DataSync:
    def __init__(self):
        url: str = os.environ.get("SUPABASE_URL")
        key: str = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set.")
        self.supabase: Client = create_client(url, key)

    def sync_offer(self, offer: dict):
        """
        Syncs an offer to Supabase.
        Checks for duplicates based on store, product_name, and valid_until.
        """
        try:
            # Check for existing record to avoid duplicates
            # Using store + product_name + valid_until + price as a 'composite key' for uniqueness check
            match_criteria = {
                "store": offer.get("store"),
                "product_name": offer.get("product_name"),
                "valid_until": offer.get("valid_until"),
            }
            
            # If valid_until is None, maybe skip that check or handle it? 
            # For now assuming valid_until is present as per schema
            
            response = self.supabase.table("offers").select("id").match(match_criteria).execute()

            if response.data and len(response.data) > 0:
                print(f"Create/Update skipped (Duplicate): {offer.get('product_name')} at {offer.get('store')}")
                # Optional: Update request if price changed? 
                # For now just skip as 'upsert' usually implies 'insert or update', but without ID we can't easily update specific row via simple upsert call without unique constraint.
                return

            self.supabase.table("offers").insert(offer).execute()
            print(f"Inserted: {offer.get('product_name')}")

        except Exception as e:
            print(f"DataSync Error: {e}")
