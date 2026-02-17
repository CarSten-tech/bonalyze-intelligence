from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, Optional

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

    # Scraper Config
    API_HOST: str = "api.marktguru.de"
    ZIP_CODE: str = "41460"
    DEFAULT_TIMEOUT: int = 10
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 1
    
settings = Settings()
