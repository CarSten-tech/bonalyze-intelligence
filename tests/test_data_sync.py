from datetime import datetime

from data_sync import DataSync
from models import BonalyzeOffer


def _offer(product_name: str, offer_id: str) -> BonalyzeOffer:
    return BonalyzeOffer(
        retailer="edeka",
        product_name=product_name,
        price=1.99,
        regular_price=2.49,
        currency="EUR",
        valid_from=datetime(2026, 2, 15),
        valid_to=datetime(2026, 2, 21),
        image_url="https://example.com/image.webp",
        source_url="https://www.marktguru.de/angebote/edeka/123",
        offer_id=offer_id,
        embedding=[0.1] * 768,
    )


def test_build_offer_row_sets_required_db_fields():
    row = DataSync._build_offer_row(_offer("  Frikadelle  im   Brötchen Stück ", "123"))
    assert row["store"] == "edeka"
    assert row["product_name"] == "Frikadelle im Brötchen Stück"
    assert row["original_price"] == 2.49
    assert row["valid_until"] is not None
    assert row["product_slug"] == "frikadelle-im-brotchen-stuck"
    assert row["source_url"] == "https://www.marktguru.de/angebote/edeka/123"


def test_build_offer_row_uses_offer_id_slug_fallback():
    row = DataSync._build_offer_row(_offer("💥", "abc-99"))
    assert row["product_slug"] == "offer-abc-99"


def test_build_offer_row_uses_source_url_fallback():
    offer = BonalyzeOffer(
        retailer="aldi-sued",
        product_name="Knabber Box",
        price=1.99,
        regular_price=2.49,
        currency="EUR",
        valid_from=datetime(2026, 2, 15),
        valid_to=datetime(2026, 2, 21),
        image_url="https://example.com/image.webp",
        source_url=None,
        offer_id="21736191",
        embedding=[0.1] * 768,
    )
    row = DataSync._build_offer_row(offer)
    assert row["source_url"] == "https://www.marktguru.de/angebote/aldi-sued/21736191"
