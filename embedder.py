import os
import logging
from google import genai
from supabase import create_client, Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict
import time

from config import settings

logger = logging.getLogger(__name__)

class Embedder:
    def __init__(self):
        # Initialize Supabase
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

        # Initialize Gemini
        if not settings.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY must be set in environment variables.")
        
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY, http_options={'api_version': 'v1'})
        self.model = settings.GEMINI_EMBEDDING_MODEL

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _generate_embeddings_api(self, texts: List[str]) -> List[List[float]]:
        """
        Call Gemini API with retry logic using true batching.
        Enhanced to handle partial failures by falling back to individual calls if batch fails.
        """
        if not texts:
            return []
        
        embeddings = []
        BATCH_SIZE = settings.EMBEDDING_BATCH_SIZE
        
        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i:i+BATCH_SIZE]
            
            try:
                # Attempt batch embedding
                result = self.client.models.embed_content(
                    model=self.model,
                    contents=batch,
                    config={'title': "Product Embedding"} 
                )
                
                if result.embeddings:
                    embeddings.extend([emb.values for emb in result.embeddings])
                else:
                     logger.warning(f"Embedder: Batch {i//BATCH_SIZE} returned no embeddings. Attempting individual fallback.")
                     embeddings.extend(self._generate_individual_fallback(batch))
                     
            except Exception as e:
                # 404 Handling: Warning only, don't crash the scraper
                if "404" in str(e):
                    logger.warning(f"Embedder: Model not found (404) for batch {i//BATCH_SIZE}. Skipping batch. Error: {e}")
                    # Return empty embeddings for this batch to keep the scraper running
                    embeddings.extend([[] for _ in batch])
                else:
                    logger.error(f"Embedder: Batch {i//BATCH_SIZE} failed: {e}. Attempting individual fallback.")
                    # If batch fails (non-404), try each text individually to save as many as possible
                    embeddings.extend(self._generate_individual_fallback(batch))
                
        return embeddings

    def _generate_individual_fallback(self, texts: List[str]) -> List[List[float]]:
        """Fallback to generate embeddings one by one if batch fails."""
        safe_embeddings = []
        for text in texts:
            try:
                result = self.client.models.embed_content(
                    model=self.model,
                    contents=text,
                    config={'title': "Product Embedding"}
                )
                if result.embeddings and len(result.embeddings) > 0:
                    safe_embeddings.append(result.embeddings[0].values)
                else:
                    logger.error(f"Embedder: Could not generate embedding for text: {text[:50]}...")
                    safe_embeddings.append([]) # Append empty to maintain index/zip alignment if needed, or handle upstream
            except Exception as e:
                logger.error(f"Embedder: Failed individual embedding for '{text[:50]}...': {e}")
                safe_embeddings.append([])
        return safe_embeddings

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
            unique_texts = list(set(texts))
            CACHE_LOOKUP_CHUNK = 200
            for i in range(0, len(unique_texts), CACHE_LOOKUP_CHUNK):
                chunk = unique_texts[i:i+CACHE_LOOKUP_CHUNK]
                response = self.supabase.table("product_embeddings_cache").select("name, embedding").in_("name", chunk).execute()
                
                if response.data:
                    for row in response.data:
                        results[row["name"]] = row["embedding"]

        except Exception as e:
            logger.warning(f"Cache lookup error: {e}")
        
        # Identify missing
        for text in unique_texts:
            if text not in results:
                missing_texts.append(text)

        if not missing_texts:
            return results

        logger.info(f"Generating {len(missing_texts)} new embeddings...")

        # 2. Generate Missing
        try:
            new_embeddings = self._generate_embeddings_api(missing_texts)
            
            # 3. Cache New
            upsert_data = []
            for text, emb in zip(missing_texts, new_embeddings):
                results[text] = emb
                if emb and len(emb) > 0: # Robustness: Only cache if we actually have dimensions
                    upsert_data.append({"name": text, "embedding": emb})
            
            if upsert_data:
                UPSERT_CHUNK = 100
                for i in range(0, len(upsert_data), UPSERT_CHUNK):
                    self.supabase.table("product_embeddings_cache").upsert(upsert_data[i:i+UPSERT_CHUNK]).execute()
            else:
                logger.warning("Embedder: No valid embeddings were generated in this batch. Skipping cache upsert.")
                    
        except Exception as e:
            logger.error(f"Generation/Caching error: {e}")

        return results

    def get_embedding(self, text: str) -> list[float]:
        """Legacy/Single method wrapper"""
        res = self.get_embeddings_batch([text])
        return res.get(text, [])
