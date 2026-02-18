from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # API Keys (Optional fallbacks, Sentinel discovers them dynamically)
    MARKETGURU_API_KEY: Optional[str] = None
    MARKETGURU_CLIENT_KEY: Optional[str] = None
    
    # Supabase
    SUPABASE_URL: str
    SUPABASE_KEY: str
    
    # Gemini
    GEMINI_API_KEY: str
    GEMINI_API_VERSION: str = "v1beta"

    # Scraper Config
    API_HOST: str = "api.marktguru.de"
    ZIP_CODE: str = "41460"
    DEFAULT_TIMEOUT: int = 10
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 1
    SCRAPER_BATCH_SIZE: int = 50
    SCRAPER_DELAY_MIN_SEC: float = 1
    SCRAPER_DELAY_MAX_SEC: float = 3
    ALLOWED_STORES: str = "kaufland,aldi-sued,edeka"
    
    # Sentinel Config
    SENTINEL_TIMEOUT: int = 120000
    
    # Embedder Config
    GEMINI_EMBEDDING_MODEL: str = "gemini-embedding-001"
    EMBEDDING_BATCH_SIZE: int = 100

    # Runtime quality policy
    FAIL_ON_PARTIAL_SYNC: bool = True
    MAX_FAILURE_RATE: float = 0.35
    
settings = Settings()
