from typing import List


def parse_allowed_stores(raw: str) -> List[str]:
    stores = [s.strip() for s in raw.split(",") if s.strip()]
    return stores or ["kaufland", "aldi-sued", "edeka"]
