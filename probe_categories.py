import asyncio
import logging
from sentinel import Sentinel
from scraper import Scraper
from config import settings

# Logging auf INFO stellen, um nur das Wichtigste zu sehen
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("CategoryProbe")

async def probe_categories():
    logger.info("🚀 Starte Kategorie-Analyse...")
    
    # 1. Header via Sentinel holen
    sentinel = Sentinel(headless=True)
    headers = await sentinel.extract_headers()
    
    if not headers:
        logger.error("❌ Keine Header gefunden. Prüfe deine Verbindung.")
        return

    # 2. Scraper initialisieren
    scraper = Scraper(discovered_headers=headers)
    scraper.load_retailer_configs()
    
    # Wir testen mit Kaufland (ID 126654), da dort die meiste Varianz herrscht
    retailer_key = "kaufland"
    retailer_id = scraper.retailer_mapping.get(retailer_key)
    
    if not retailer_id:
        logger.error(f"❌ Retailer '{retailer_key}' nicht in DB gefunden.")
        return

    logger.info(f"🔍 Rufe Test-Daten für {retailer_key} ab...")
    
    # 3. Rohdaten abrufen (einfacher API-Call ohne Pydantic-Parsing)
    url = f"https://{settings.API_HOST}/api/v1/offers"
    params = {
        "retailerIds": retailer_id,
        "zipCode": settings.ZIP_CODE,
        "limit": 500, # Große Stichprobe für viele Kategorien
        "offset": 0
    }
    
    try:
        response = scraper.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        categories = {}

        # 4. Kategorien extrahieren
        for item in results:
            cat = item.get("category")
            if cat:
                cat_id = cat.get("id")
                cat_name = cat.get("name")
                if cat_id not in categories:
                    categories[cat_id] = cat_name

        # 5. Ergebnis-Ausgabe
        logger.info("\n✅ Gefundene Kategorien bei Marktguru:")
        logger.info("-" * 40)
        logger.info(f"{'ID':<10} | {'Kategorie-Name'}")
        logger.info("-" * 40)
        
        # Sortiert nach Name für bessere Übersicht
        for cid in sorted(categories.keys()):
            logger.info(f"{cid:<10} | {categories[cid]}")
            
        logger.info("-" * 40)
        logger.info(f"Total: {len(categories)} verschiedene Kategorien gefunden.")

    except Exception as e:
        logger.error(f"❌ Fehler beim API-Call: {e}")

if __name__ == "__main__":
    asyncio.run(probe_categories())