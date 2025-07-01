from datetime import date
from pydantic import BaseModel, Field


class InventoryItem(BaseModel):
    """
    Defines the data contract for a single, normalized row in our final report.
    This "long" format is highly scalable and analytics-friendly.
    """

    sku: str = Field(..., alias="SKU")
    channel: str = Field(..., alias="Channel")
    units_sold: int = Field(default=0, ge=0, alias="Units Sold")
    inventory: int = Field(default=0, ge=0, alias="Inventory")
    inbound: int = Field(default=0, ge=0, alias="Inbound")
    last_updated: date = Field(..., alias="Last Updated")

    class Config:
        # This config ensures we can create models from DataFrame rows (dicts)
        # and that when we export to JSON, it uses our friendly aliases.
        populate_by_name = True
