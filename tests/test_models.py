from models import BonalyzeOffer


def test_bonalyze_offer_normalizes_fields():
    offer = BonalyzeOffer(
        retailer="edeka",
        product_name="  Frikadelle   im Brötchen  ",
        price=1.99,
        regular_price=1.49,  # should be clamped to current price
        offer_id="  123  ",
    )
    assert offer.product_name == "Frikadelle im Brötchen"
    assert offer.offer_id == "123"
    assert offer.regular_price == 1.99


def test_bonalyze_offer_rejects_empty_product_name():
    try:
        BonalyzeOffer(
            retailer="edeka",
            product_name="   ",
            price=1.0,
            regular_price=1.0,
            offer_id="1",
        )
    except ValueError:
        return
    raise AssertionError("Expected ValueError for empty product_name")
