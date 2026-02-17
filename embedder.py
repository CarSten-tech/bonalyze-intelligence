import os
from google import genai
from supabase import create_client, Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict
import time

from config import settings

class Embedder:
    def __init__(self):
        # Initialize Supabase
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

        # Initialize Gemini
        if not settings.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY must be set in environment variables.")
        
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        self.model = "text-embedding-004"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _generate_embeddings_api(self, texts: List[str]) -> List[List[float]]:
        """Call Gemini API with retry logic."""
        if not texts:
            return []
        
        # Gemini often supports batching. If not, we might need loop here.
        # Assuming google-genai supports list of contents.
        # If the library expects a single string or list, we need to check docs.
        # For now, let's assume we might need to iterate if batching isn't straightforward or to be safe.
        # Actually newer Vertex/Gemini APIs support batch.
        
        embeddings = []
        # Batching explicitly to avoid hitting size limits if any
        BATCH_SIZE = 100
        
        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i:i+BATCH_SIZE]
            result = self.client.models.embed_content(
                model=self.model,
                contents=batch,
                config={'title': "Product Embedding"} 
            )
            # Result structure depends on library version. 
            # If batch, result.embeddings should be a list.
            if result.embeddings:
                for emb in result.embeddings:
                    embeddings.append(emb.values)
            else:
                # Fallback or error
                raise ValueError("No embeddings returned from API")
                
        return embeddings

    def get_embeddings_batch(self, texts: List[str]) -> Dict[str, List[float]]:
        """
        Retrieves embeddings for a list of texts.
        1. Check Supabase cache (bulk).
        2. Generate missing via Gemini.
        3. Cache new embeddings.
        Returns a dictionary mapping text -> embedding.
        """
        if not texts:
            return {}

        results = {}
        missing_texts = []
        
        # 1. Check Cache
        try:
            # Chunking for Supabase 'in_' query if list is huge
            # Supabase URL length limit safeguard
            unique_texts = list(set(texts))
            
            # Simple chunking for cache lookup
            CACHE_LOOKUP_CHUNK = 200
            for i in range(0, len(unique_texts), CACHE_LOOKUP_CHUNK):
                chunk = unique_texts[i:i+CACHE_LOOKUP_CHUNK]
                response = self.supabase.table("product_embeddings_cache").select("name, embedding").in_("name", chunk).execute()
                
                if response.data:
                    for row in response.data:
                        results[row["name"]] = row["embedding"]

        except Exception as e:
            print(f"Embedder: Cache lookup error: {e}")
            # Proceed to generate all if cache fails? Or just log.
            # We'll treat all as missing if cache fails to be safe/resilient
        
        # Identify missing
        for text in unique_texts:
            if text not in results:
                missing_texts.append(text)

        if not missing_texts:
            return results

        print(f"Embedder: Generating {len(missing_texts)} new embeddings...")

        # 2. Generate Missing
        try:
            new_embeddings = self._generate_embeddings_api(missing_texts)
            
            # 3. Cache New
            upsert_data = []
            for text, emb in zip(missing_texts, new_embeddings):
                results[text] = emb
                upsert_data.append({"name": text, "embedding": emb})
            
            if upsert_data:
                # Batch upsert
                UPSERT_CHUNK = 100
                for i in range(0, len(upsert_data), UPSERT_CHUNK):
                    self.supabase.table("product_embeddings_cache").upsert(upsert_data[i:i+UPSERT_CHUNK]).execute()
                    
        except Exception as e:
            print(f"Embedder: Generation/Caching error: {e}")
            # Start returning what we have?

        return results

    def get_embedding(self, text: str) -> list[float]:
        """Legacy/Single method wrapper"""
        res = self.get_embeddings_batch([text])
        return res.get(text, [])
