from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # API Keys
    MARKETGURU_API_KEY: str = "8Kk+pmbf7TgJ9nVj2cXeA7P5zBGv8iuutVVMRfOfvNE="
    MARKETGURU_CLIENT_KEY: str = "WU/RH+PMGDi+gkZer3WbMelt6zcYHSTytNB7VpTia90="
    
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
    
    # Retailer Mapping
    RETAILER_IDS: Dict[str, str] = {
        "kaufland": "126654",
        "aldi_sued": "127153",
        "edeka": "126699",
        "lidl": "126679"
    }

settings = Settings()
