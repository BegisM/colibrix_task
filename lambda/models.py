from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator, ValidationError


class CardTransaction(BaseModel):
    """
    Pydantic model that describes a single card transaction.

    This model encodes all validation rules from the assignment:
    - non-empty id and organization_id
    - amount >= 0
    - currency in [EUR, USD, GBP]
    - status in [Success, Error, Pending]
    - created_at is a valid ISO8601 UTC timestamp
    """

    id: str = Field(min_length=1)
    organization_id: str = Field(min_length=1)
    amount: int = Field(ge=0)
    currency: Literal["EUR", "USD", "GBP"]
    status: Literal["Success", "Error", "Pending"]
    created_at: datetime

    @field_validator("id", "organization_id")
    @classmethod
    def not_empty(cls, v: str) -> str:
        """Extra safety: disallow strings with just spaces."""
        if not v or not v.strip():
            raise ValueError("must be non-empty")
        return v

    @field_validator("created_at")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        """
        The assignment explicitly requires a UTC timestamp.
        Here I check that the datetime has UTC timezone.
        """
        if v.tzinfo is None or v.utcoffset() != timezone.utc.utcoffset(v):
            raise ValueError("created_at must be a UTC timestamp")
        return v


def validate_record(raw: dict):
    """
    Try to parse and validate a CSV row.

    Returns:
        (True, model_instance, None)   -> when the row is valid
        (False, original_row, errors)  -> when the row is invalid

    I keep this helper small and explicit so the Lambda code
    can focus only on “happy path vs invalid path” logic.
    """
    try:
        obj = CardTransaction.model_validate(raw)
        return True, obj, None
    except ValidationError as e:
        return False, raw, e.errors()
