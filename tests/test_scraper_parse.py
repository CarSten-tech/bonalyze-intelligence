from scraper import Scraper
from models import BonalyzeOffer


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
    assert offer.category == "Lebensmittel > Milchprodukte & Eier"


def test_parse_offer_extracts_category_from_categories_list():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(categories=[{"id": 163, "name": "Käse"}]),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Lebensmittel > Milchprodukte & Eier"


def test_parse_offer_extracts_category_from_product_categories():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(product={"id": 501, "name": "Frikadelle", "categories": [{"name": "Molkerei"}]}),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Lebensmittel > Milchprodukte & Eier"


def test_parse_offer_extracts_category_from_category_name_field():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(categoryName="Backwaren"),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Lebensmittel > Brot & Backwaren"


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
    assert offer.category == "Lebensmittel > Milchprodukte & Eier"


def test_enrich_categories_with_global_offers_by_offer_and_product():
    scraper = object.__new__(Scraper)
    scraper._global_offer_categories_by_offer_id = {"offer-1": "Käse"}
    scraper._global_offer_categories_by_product_id = {"prod-2": "Brot"}
    scraper._global_offer_categories_by_product_name = {}
    scraper._global_category_index_loaded = True

    offer_1 = BonalyzeOffer(
        retailer="edeka",
        product_name="Produkt 1",
        price=1.0,
        regular_price=1.0,
        offer_id="offer-1",
        raw_data={"product": {"id": "prod-1"}},
    )
    offer_2 = BonalyzeOffer(
        retailer="edeka",
        product_name="Produkt 2",
        price=1.0,
        regular_price=1.0,
        offer_id="offer-2",
        raw_data={"product": {"id": "prod-2"}},
    )
    offers = [offer_1, offer_2]

    scraper._enrich_categories_with_global_offers(offers, "edeka")

    assert offer_1.category == "Lebensmittel > Milchprodukte & Eier"
    assert offer_2.category == "Lebensmittel > Brot & Backwaren"


def test_parse_offer_maps_getraenke_top_category():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(category={"name": "Bier"}),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Getränke > Alkohol"


def test_parse_offer_maps_getraenke_alkoholfrei():
    scraper = object.__new__(Scraper)
    offer = scraper._parse_offer(
        _valid_item(category={"name": "Wasser"}),
        "edeka",
    )
    assert offer is not None
    assert offer.category == "Getränke > Alkoholfrei"


def test_parse_offer_maps_food_subcategories():
    scraper = object.__new__(Scraper)
    assert scraper._to_category_label("Gemüse", "Rispentomaten") == "Lebensmittel > Gemüse"
    assert scraper._to_category_label("Obst", "Äpfel") == "Lebensmittel > Obst"
    assert scraper._to_category_label("Tiefkühl", "TK Pizza") == "Lebensmittel > Tiefkühl"
    assert scraper._to_category_label("Konserve", "Dosentomaten") == "Lebensmittel > Konserven & Haltbares"
    assert scraper._to_category_label(None, "Zuckererbsen Ägypt. Zuckererbsen Kl. I je 200-g-Packg.") == "Lebensmittel > Gemüse"
    assert scraper._to_category_label(None, "Haferflocken 100 % Vollkorn je 500-g-Packg.") == "Lebensmittel > Grundnahrungsmittel"
    assert scraper._to_category_label(None, "Rispentomaten Dtsch. Kl. I je 650-g-Packg.") == "Lebensmittel > Gemüse"
    assert scraper._to_category_label(None, "XXL Hähnchenflügel HKL A je 1-kg-Großpackg.") == "Lebensmittel > Fleisch, Wurst & Fisch"
    assert scraper._to_category_label(None, "Schlagrahm mind. 32 % Fett je 500-g-Packg.") == "Lebensmittel > Milchprodukte & Eier"
    assert scraper._to_category_label(None, "Bitter ital. Aperitif 25 Vol. % je 0,7-l-Fl.") == "Getränke > Alkohol"
