from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List, Any
from datetime import datetime
from normalization import normalize_whitespace

class OfferImage(BaseModel):
    id: Optional[int] = None
    images: Optional[dict] = None # Or more specific if needed

class MarktguruProduct(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    brand: Optional[str] = None

class MarktguruOfferUnit(BaseModel):
    id: int
    shortName: Optional[str] = None

class MarktguruValidityDate(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime

class MarktguruRetailer(BaseModel):
    id: int
    name: str
    indexOffer: bool = False

class MarktguruCategory(BaseModel):
    id: int
    name: str

class MarktguruOffer(BaseModel):
    model_config = ConfigDict(extra='ignore')
    
    id: int
    product: MarktguruProduct
    retailer: Optional[MarktguruRetailer] = None
    category: Optional[Any] = None
    price: float
    oldPrice: Optional[float] = None
    referencePrice: Optional[float] = None
    description: Optional[str] = None
    quantity: Optional[Any] = None
    unit: Optional[MarktguruOfferUnit] = None
    validityDates: List[MarktguruValidityDate] = []
    validFrom: Optional[datetime] = Field(None, alias="validFrom")
    validTo: Optional[datetime] = Field(None, alias="validTo")
    images: Optional[dict] = None # Metadata about images

class BonalyzeOffer(BaseModel):
    model_config = ConfigDict(extra='forbid')

    retailer: str
    product_name: str
    price: float = Field(ge=0)
    regular_price: float = Field(ge=0)
    unit: Optional[str] = None
    amount: Optional[Any] = None
    currency: str = "EUR"
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    image_url: Optional[str] = None
    source_url: Optional[str] = None
    offer_id: str
    embedding: Optional[List[float]] = None
    scraped_at: datetime = Field(default_factory=datetime.now)
    raw_data: Optional[dict] = None

    @field_validator("product_name", mode="before")
    @classmethod
    def _normalize_product_name(cls, value: Any) -> str:
        text = normalize_whitespace(str(value or ""))
        if not text:
            raise ValueError("product_name must not be empty")
        return text

    @field_validator("offer_id", mode="before")
    @classmethod
    def _normalize_offer_id(cls, value: Any) -> str:
        text = normalize_whitespace(str(value or ""))
        if not text:
            raise ValueError("offer_id must not be empty")
        return text

    @field_validator("regular_price", mode="after")
    @classmethod
    def _clamp_regular_price(cls, value: float, info):
        price = info.data.get("price")
        if isinstance(price, (int, float)) and value < float(price):
            return float(price)
        return float(value)
