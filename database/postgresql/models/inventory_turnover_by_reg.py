from pydantic import BaseModel, field_validator


class QuantityAndSupply(BaseModel):
    article_id: int
    quantity: int
    supply_qty: int
    # supply_count: int

    # @field_validator('quantity')
    # def check_quantity(cls, value):
    #     if value < 0:
    #         raise ValueError("Quantity cannot be negative")
    #     return value