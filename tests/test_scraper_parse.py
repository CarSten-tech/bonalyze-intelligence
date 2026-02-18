from scraper import Scraper


def _valid_item(**overrides):
    item = {
        "id": 21755305,
        "product": {"id": 501, "name": "Frikadelle"},
        "retailer": {"id": 4, "name": "EDEKA", "indexOffer": True},
        "price": 3.0,
        "oldPrice": None,
        "description": "im Brötchen Stück",
        "validFrom": "2026-02-15T00:00:00+00:00",
        "validTo": "2026-02-21T23:59:00+00:00",
    }
    item.update(overrides)
    return item


def test_parse_offer_falls_back_regular_price_to_price():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(_valid_item(), "edeka")
    assert offer is not None
    assert offer.price == 3.0
    assert offer.regular_price == 3.0
    assert offer.product_name == "Frikadelle im Brötchen Stück"
    assert offer.source_url == "https://www.marktguru.de/angebote/edeka/21755305"


def test_parse_offer_rejects_missing_validity():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(validFrom=None, validTo=None, validityDates=[]),
        "edeka",
    )
    assert offer is None


def test_parse_offer_prefers_payload_source_url():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(sourceUrl="https://example.com/offers/21755305"),
        "edeka",
    )
    assert offer is not None
    assert offer.source_url == "https://example.com/offers/21755305"
