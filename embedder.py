import logging
from google import genai
from google.genai import types
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict

from config import settings

logger = logging.getLogger(__name__)

class Embedder:
    EMBEDDING_DIMENSION = 768
    MAX_SKIPPED_LOG_SAMPLES = 5
    FALLBACK_EMBEDDING_MODEL = "gemini-embedding-001"

    def __init__(self):
        # Initialize Supabase
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")
        self.supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

        # Initialize Gemini
        if not settings.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY must be set in environment variables.")
        
        self.model = settings.GEMINI_EMBEDDING_MODEL
        api_version = settings.GEMINI_API_VERSION
        if self.model == "text-embedding-004" and api_version == "v1":
            logger.warning("Embedder: text-embedding-004 is not supported on api_version=v1 for embedContent. Forcing v1beta.")
            api_version = "v1beta"
        self.api_version = api_version
        self.client = genai.Client(
            api_key=settings.GEMINI_API_KEY,
            http_options=types.HttpOptions(api_version=self.api_version),
        )
        self._models_list_logged = False
        logger.info(f"Embedder initialized with model='{self.model}', api_version='{self.api_version}'.")

    def _is_404_error(self, error: Exception) -> bool:
        status_code = getattr(error, "status_code", None)
        if status_code == 404:
            return True

        code = getattr(error, "code", None)
        if code == 404:
            return True

        response = getattr(error, "response", None)
        if response is not None and getattr(response, "status_code", None) == 404:
            return True

        return "404" in str(error)

    def _is_valid_embedding(self, values: List[float] | None) -> bool:
        return bool(values) and len(values) == self.EMBEDDING_DIMENSION

    def _list_embedding_capable_models(self) -> List[str]:
        try:
            models = self.client.models.list(config={"page_size": 100})
            embedding_models: List[str] = []
            for m in models:
                supported_actions = set(getattr(m, "supported_actions", []) or [])
                if "embedContent" in supported_actions or "batchEmbedContents" in supported_actions:
                    name = getattr(m, "name", None)
                    if name:
                        embedding_models.append(name)
            return embedding_models
        except Exception as e:
            logger.warning(f"Embedder diagnostics: ListModels failed: {e}")
            return []

    def _log_available_embedding_models_once(self) -> None:
        if self._models_list_logged:
            return
        self._models_list_logged = True

        embedding_models = self._list_embedding_capable_models()
        if embedding_models:
            preview = ", ".join(embedding_models[:20])
            logger.warning(
                f"Embedder diagnostics: embedding-capable models visible for current key/provider: {preview}"
            )
        else:
            logger.warning(
                "Embedder diagnostics: no embedding-capable models returned by ListModels for current key/provider."
            )

    def _switch_to_fallback_model_if_available(self) -> bool:
        if self.model == self.FALLBACK_EMBEDDING_MODEL:
            return False

        embedding_models = self._list_embedding_capable_models()
        normalized = {name.replace("models/", "") for name in embedding_models}
        if self.FALLBACK_EMBEDDING_MODEL in normalized:
            old_model = self.model
            self.model = self.FALLBACK_EMBEDDING_MODEL
            logger.warning(
                f"Embedder: Switching model from '{old_model}' to '{self.model}' after 404."
            )
            return True

        return False

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
                    config={"output_dimensionality": self.EMBEDDING_DIMENSION}
                )

                batch_embeddings = getattr(result, "embeddings", None)
                if not batch_embeddings or len(batch_embeddings) != len(batch):
                    logger.warning(
                        f"Embedder: Batch {i//BATCH_SIZE} returned empty/incomplete embeddings. Attempting individual fallback."
                    )
                    embeddings.extend(self._generate_individual_fallback(batch))
                    continue

                for text, emb in zip(batch, batch_embeddings):
                    values = getattr(emb, "values", None)
                    if self._is_valid_embedding(values):
                        embeddings.append(values)
                    else:
                        logger.warning(
                            f"Embedder: Invalid embedding dimension for '{text[:50]}...'. Attempting individual fallback."
                        )
                        embeddings.extend(self._generate_individual_fallback([text]))
                     
            except Exception as e:
                # 404 Handling: Warning only, don't crash the scraper
                if self._is_404_error(e):
                    logger.warning(
                        f"Embedder: Model not found (404) for batch {i//BATCH_SIZE} (model={self.model}, api={self.api_version}). "
                        "Trying fallback model if available."
                    )
                    if self._switch_to_fallback_model_if_available():
                        try:
                            result = self.client.models.embed_content(
                                model=self.model,
                                contents=batch,
                                config={"output_dimensionality": self.EMBEDDING_DIMENSION}
                            )

                            batch_embeddings = getattr(result, "embeddings", None)
                            if not batch_embeddings or len(batch_embeddings) != len(batch):
                                logger.warning(
                                    f"Embedder: Fallback model returned empty/incomplete batch {i//BATCH_SIZE}. "
                                    "Attempting individual fallback."
                                )
                                embeddings.extend(self._generate_individual_fallback(batch))
                                continue

                            for text, emb in zip(batch, batch_embeddings):
                                values = getattr(emb, "values", None)
                                if self._is_valid_embedding(values):
                                    embeddings.append(values)
                                else:
                                    logger.warning(
                                        f"Embedder: Invalid embedding dimension with fallback model for '{text[:50]}...'. "
                                        "Attempting individual fallback."
                                    )
                                    embeddings.extend(self._generate_individual_fallback([text]))
                            continue
                        except Exception as fallback_error:
                            if not self._is_404_error(fallback_error):
                                logger.error(
                                    f"Embedder: Fallback retry failed for batch {i//BATCH_SIZE}: {fallback_error}. "
                                    "Attempting individual fallback."
                                )
                                embeddings.extend(self._generate_individual_fallback(batch))
                                continue
                            logger.warning(
                                f"Embedder: Fallback model also returned 404 for batch {i//BATCH_SIZE}."
                            )

                    self._log_available_embedding_models_once()
                    remaining_items = len(texts) - len(embeddings)
                    embeddings.extend([[] for _ in range(remaining_items)])
                    break
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
                    config={"output_dimensionality": self.EMBEDDING_DIMENSION}
                )

                batch_embeddings = getattr(result, "embeddings", None)
                if batch_embeddings and len(batch_embeddings) > 0:
                    values = getattr(batch_embeddings[0], "values", None)
                    if self._is_valid_embedding(values):
                        safe_embeddings.append(values)
                    else:
                        logger.warning(f"Embedder: Invalid embedding shape for text: {text[:50]}...")
                        safe_embeddings.append([])
                else:
                    logger.warning(f"Embedder: Could not generate embedding for text: {text[:50]}...")
                    safe_embeddings.append([])
            except Exception as e:
                if self._is_404_error(e):
                    logger.warning(f"Embedder: Model not found (404) for text '{text[:50]}...'.")
                    self._log_available_embedding_models_once()
                else:
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
        unique_texts = list(set(texts))
        
        # 1. Check Cache
        try:
            CACHE_LOOKUP_CHUNK = 200
            for i in range(0, len(unique_texts), CACHE_LOOKUP_CHUNK):
                chunk = unique_texts[i:i+CACHE_LOOKUP_CHUNK]
                response = self.supabase.table("product_embeddings_cache").select("name, embedding").in_("name", chunk).execute()
                
                if response.data:
                    for row in response.data:
                        emb = row.get("embedding")
                        if self._is_valid_embedding(emb):
                            results[row["name"]] = emb

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
            if len(new_embeddings) < len(missing_texts):
                new_embeddings.extend([[] for _ in range(len(missing_texts) - len(new_embeddings))])
            elif len(new_embeddings) > len(missing_texts):
                new_embeddings = new_embeddings[:len(missing_texts)]
            
            # 3. Cache New
            upsert_data = []
            skipped_texts = []
            for text, emb in zip(missing_texts, new_embeddings):
                if self._is_valid_embedding(emb):
                    results[text] = emb
                    upsert_data.append({"name": text, "embedding": emb})
                else:
                    skipped_texts.append(text)

            if skipped_texts:
                preview = ", ".join(f"'{t[:40]}...'" for t in skipped_texts[: self.MAX_SKIPPED_LOG_SAMPLES])
                logger.warning(
                    f"Embedder: Skipped {len(skipped_texts)} items without valid {self.EMBEDDING_DIMENSION}-dim embeddings. "
                    f"Examples: {preview}"
                )
            
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
