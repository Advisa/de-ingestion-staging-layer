from pydantic import BaseModel
from typing import Optional

class AnonymizationEvent(BaseModel):
    national_id: str
    customer_id: Optional[int]
    emails: Optional[list]
    mobile_phones: Optional[list]
    market: str
    last_brand_interaction: str
    compliance_event: str

    def to_json(self):
        """Convert model instance to JSON dictionary."""
        return self.model_dump()
