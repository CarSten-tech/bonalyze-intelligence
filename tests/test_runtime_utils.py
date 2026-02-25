from runtime_utils import parse_allowed_stores


def test_parse_allowed_stores_strips_and_splits():
    stores = parse_allowed_stores(" kaufland , aldi-sued,edeka ")
    assert stores == ["kaufland", "aldi-sued", "edeka"]


def test_parse_allowed_stores_fallback_when_empty():
    stores = parse_allowed_stores("  ,  ")
    assert stores == ["kaufland", "aldi-sued", "edeka"]
