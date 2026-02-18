import re
import unicodedata


def normalize_whitespace(value: str) -> str:
    """Collapse repeated whitespace and trim."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def slugify(value: str) -> str:
    """Create ASCII slug from arbitrary text."""
    normalized = normalize_whitespace(value)
    if not normalized:
        return ""

    ascii_value = unicodedata.normalize("NFKD", normalized).encode("ascii", "ignore").decode("ascii")
    ascii_value = ascii_value.lower()
    ascii_value = re.sub(r"[^a-z0-9]+", "-", ascii_value)
    ascii_value = re.sub(r"-{2,}", "-", ascii_value).strip("-")
    return ascii_value
