from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List, Any
from datetime import datetime

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
    category: Optional[MarktguruCategory] = None
    price: float
    oldPrice: Optional[float] = None
    referencePrice: Optional[float] = None
    description: Optional[str] = None
    quantity: Optional[Any] = None
    unit: Optional[MarktguruOfferUnit] = None
    validityDates: List[MarktguruValidityDate] = []
    validFrom: Optional[datetime] = None
    validTo: Optional[datetime] = None
    images: Optional[dict] = None # Metadata about images

class BonalyzeOffer(BaseModel):
    retailer: str
    product_name: str
    price: float
    regular_price: float
    unit: Optional[str] = None
    amount: Optional[Any] = None
    currency: str = "EUR"
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    image_url: Optional[str] = None
    offer_id: str
    embedding: Optional[List[float]] = None
    scraped_at: datetime = Field(default_factory=datetime.now)
    raw_data: Optional[dict] = None

