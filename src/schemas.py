from datetime import date
from pydantic import BaseModel, Field


class InventoryItem(BaseModel):
    """
    Normalized Inventory Record.
    Aligned with SalesRecord structure.
    """

    # ID Structure: YYYYMMDD_Channel_SKU
    id: str = Field(..., description="Unique Entry ID")
    sku_channel_id: str = Field(..., description="Channel_SKU")

    report_date: date = Field(..., alias="Date")
    sku: str = Field(..., alias="SKU")
    channel: str = Field(..., alias="Channel")

    units_sold: int = Field(default=0, ge=0, alias="Units Sold")
    inventory: int = Field(default=0, ge=0, alias="Inventory")
    inbound: int = Field(default=0, ge=0, alias="Inbound")

    class Config:
        populate_by_name = True


class SalesItem(BaseModel):
    """
    Defines the data contract for the consolidated Sales Report.
    """

    sku: str = Field(..., alias="SKU")

    # Amazon
    amazon_units: int = Field(default=0, alias="Amazon (Units)")
    amazon_revenue: float = Field(default=0.0, alias="Amazon (Revenue)")

    # TikTok
    tiktok_shop_units: int = Field(default=0, alias="TikTok Shop (Units)")
    tiktok_shop_revenue: float = Field(default=0.0, alias="TikTok Shop (Revenue)")

    # TikTok via Shopify
    tiktok_shopify_units: int = Field(default=0, alias="TikTok Shopify (Units)")
    tiktok_shopify_revenue: float = Field(default=0.0, alias="TikTok Shopify (Revenue)")

    # Shopify (DTC)
    shopify_units: int = Field(default=0, alias="Shopify (Units)")
    shopify_revenue: float = Field(default=0.0, alias="Shopify (Revenue)")

    # Walmart
    walmart_units: int = Field(default=0, alias="Walmart (Units)")
    walmart_revenue: float = Field(default=0.0, alias="Walmart (Revenue)")

    # Target
    target_units: int = Field(default=0, alias="Target (Units)")
    target_revenue: float = Field(default=0.0, alias="Target (Revenue)")

    # Others
    others_units: int = Field(default=0, alias="Others (Units)")
    others_revenue: float = Field(default=0.0, alias="Others (Revenue)")

    class Config:
        populate_by_name = True


class SalesRecord(BaseModel):
    """
    Normalized Long-Format Sales Record.
    """

    id: str = Field(..., description="SystemDate_Channel_SKU")
    sku_channel_id: str = Field(..., description="Channel_SKU")

    report_date: date = Field(..., alias="Date")
    sku: str = Field(..., alias="SKU")
    channel: str = Field(..., alias="Channel")

    units: int = Field(..., alias="Units")
    revenue: float = Field(..., alias="Revenue")

    class Config:
        populate_by_name = True
