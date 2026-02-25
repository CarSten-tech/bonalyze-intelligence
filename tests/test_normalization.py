from normalization import normalize_whitespace, slugify


def test_normalize_whitespace_collapses_spaces():
    assert normalize_whitespace("  a   b\tc \n d  ") == "a b c d"


def test_slugify_handles_umlauts_and_symbols():
    assert slugify("Frikadelle im Brötchen Stück!") == "frikadelle-im-brotchen-stuck"


def test_slugify_empty_value():
    assert slugify("   ") == ""
