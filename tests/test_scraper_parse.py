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


def test_parse_offer_extracts_category_name():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(category={"id": 12, "name": "Molkerei"}),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Molkerei"


def test_parse_offer_extracts_category_from_categories_list():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(categories=[{"id": 163, "name": "Käse"}]),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Käse"


def test_parse_offer_extracts_category_from_product_categories():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(product={"id": 501, "name": "Frikadelle", "categories": [{"name": "Molkerei"}]}),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Molkerei"


def test_parse_offer_extracts_category_from_category_name_field():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(categoryName="Backwaren"),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Backwaren"


def test_parse_offer_without_retailer_block_still_parses():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(
            retailer=None,
            categories=[{"id": 1, "name": "Molkerei"}],
            validityDates=[{"from": "2026-02-15T00:00:00+00:00", "to": "2026-02-21T23:59:00+00:00"}],
            validFrom=None,
            validTo=None,
        ),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Molkerei"
