"""
Tests for InventoryPipeline.transform() and SalesPipeline.transform().

These tests call transform() directly with pre-built DataFrames from conftest.py,
bypassing file I/O entirely. This isolates the normalization, zero-fill, ID
generation, and Pydantic validation logic from the extract phase.
"""
import pytest
import pandas as pd
from datetime import date

from src import settings
from src.pipelines.inventory import InventoryPipeline
from src.pipelines.sales import SalesPipeline

MASTER_SKUS = set(str(s) for s in settings.SKU_ORDER)
SALES_CHANNELS = set(settings.SALES_CHANNEL_ORDER)


# ---------------------------------------------------------------------------
# InventoryPipeline.transform()
# ---------------------------------------------------------------------------

class TestInventoryPipelineTransform:
    def test_returns_list_of_inventory_items(self, inventory_df):
        result = InventoryPipeline().transform(inventory_df, [])
        assert result is not None
        assert isinstance(result, list)
        assert len(result) > 0

    def test_zero_fill_all_skus_present_per_channel(self, inventory_df):
        """Every SKU in SKU_ORDER must appear for every channel in CHANNEL_ORDER."""
        result = InventoryPipeline().transform(inventory_df, [])
        output_skus_by_channel = {}
        for item in result:
            output_skus_by_channel.setdefault(item.channel, set()).add(item.sku)

        # inventory transform forces full CHANNEL_ORDER; check channels present in input
        for ch in inventory_df["Channel"].unique():
            assert MASTER_SKUS == output_skus_by_channel[ch], (
                f"Channel {ch} is missing SKUs or has extra SKUs"
            )

    def test_id_format(self, inventory_df):
        """id must follow YYYYMMDD_Channel_SKU pattern."""
        result = InventoryPipeline().transform(inventory_df, [])
        today = date.today().strftime("%Y%m%d")
        for item in result:
            assert item.id.startswith(today), f"ID doesn't start with today: {item.id}"
            parts = item.id.split("_")
            assert len(parts) >= 3

    def test_sku_channel_id_format(self, inventory_df):
        """sku_channel_id must follow Channel_SKU pattern."""
        result = InventoryPipeline().transform(inventory_df, [])
        for item in result:
            assert "_" in item.sku_channel_id
            assert item.channel in item.sku_channel_id
            assert item.sku in item.sku_channel_id

    def test_all_metrics_non_negative(self, inventory_df):
        result = InventoryPipeline().transform(inventory_df, [])
        for item in result:
            assert item.inventory >= 0
            assert item.inbound >= 0
            assert item.units >= 0

    def test_known_sku_values_preserved(self, inventory_df):
        result = InventoryPipeline().transform(inventory_df, [])
        fba_1001 = next(
            (r for r in result if r.channel == "FBA" and r.sku == "1001"), None
        )
        assert fba_1001 is not None
        assert fba_1001.inventory == 100
        assert fba_1001.inbound == 10
        assert fba_1001.units == 5

    def test_zero_filled_skus_have_zero_metrics(self, inventory_df):
        """SKUs not in the input DataFrame must appear with zeros."""
        result = InventoryPipeline().transform(inventory_df, [])
        # "9001" is in SKU_ORDER but not in the fixture inventory_df
        fba_9001 = next(
            (r for r in result if r.channel == "FBA" and r.sku == "9001"), None
        )
        assert fba_9001 is not None
        assert fba_9001.inventory == 0
        assert fba_9001.inbound == 0
        assert fba_9001.units == 0

    def test_negative_inventory_fails_validation(self, inventory_df_negative):
        """A row with inventory=-1 must cause Pydantic validation to fail → None."""
        result = InventoryPipeline().transform(inventory_df_negative, [])
        assert result is None

    def test_output_count(self, inventory_df):
        """Total rows = len(SKU_ORDER) × len(CHANNEL_ORDER) (full list forced)."""
        result = InventoryPipeline().transform(inventory_df, [])
        assert len(result) == len(settings.SKU_ORDER) * len(settings.CHANNEL_ORDER)


# ---------------------------------------------------------------------------
# SalesPipeline.transform()
# ---------------------------------------------------------------------------

class TestSalesPipelineTransform:
    def _make_pipeline_with_df(self, df):
        """Helper: construct pipeline, call transform with empty bundle_rows."""
        pipeline = SalesPipeline()
        return pipeline.transform(df, [])

    def test_returns_list_of_sales_records(self, sales_df):
        result = self._make_pipeline_with_df(sales_df)
        assert result is not None
        assert isinstance(result, list)
        assert len(result) > 0

    def test_all_sales_channels_present(self, sales_df):
        """Every channel in SALES_CHANNEL_ORDER must appear in the output."""
        result = self._make_pipeline_with_df(sales_df)
        output_channels = {r.channel for r in result}
        assert SALES_CHANNELS.issubset(output_channels)

    def test_all_skus_present_per_channel(self, sales_df):
        """Every SKU in SKU_ORDER (plus "Bundles") must appear for each channel."""
        result = self._make_pipeline_with_df(sales_df)
        regular_records = [r for r in result if r.sku != "Bundles"]
        channels_seen = set(r.channel for r in regular_records)
        for ch in channels_seen:
            ch_skus = {r.sku for r in regular_records if r.channel == ch}
            assert MASTER_SKUS == ch_skus, f"Channel {ch} missing SKUs"

    def test_bundle_row_per_channel(self, sales_df):
        """A "Bundles" row must exist for every channel in SALES_CHANNEL_ORDER."""
        result = self._make_pipeline_with_df(sales_df)
        bundle_records = [r for r in result if r.sku == "Bundles"]
        bundle_channels = {r.channel for r in bundle_records}
        assert SALES_CHANNELS == bundle_channels

    def test_id_no_spaces(self, sales_df):
        """Channels with spaces (e.g. "TikTok Shop") must use underscores in id."""
        result = self._make_pipeline_with_df(sales_df)
        for r in result:
            assert " " not in r.id, f"Space found in id: {r.id}"

    def test_sku_channel_id_no_spaces(self, sales_df):
        result = self._make_pipeline_with_df(sales_df)
        for r in result:
            assert " " not in r.sku_channel_id

    def test_units_are_int(self, sales_df):
        result = self._make_pipeline_with_df(sales_df)
        for r in result:
            assert isinstance(r.units, int)

    def test_revenue_non_negative(self, sales_df):
        result = self._make_pipeline_with_df(sales_df)
        for r in result:
            assert r.revenue >= 0.0

    def test_known_values_preserved(self, sales_df):
        result = self._make_pipeline_with_df(sales_df)
        amazon_1001 = next(
            (r for r in result if r.channel == "Amazon" and r.sku == "1001"), None
        )
        assert amazon_1001 is not None
        assert amazon_1001.units == 5
        assert abs(amazon_1001.revenue - 125.0) < 0.01

    def test_zero_filled_channel_has_zero_values(self, sales_df):
        """Channels with no data in input must be zero-filled."""
        result = self._make_pipeline_with_df(sales_df)
        # "TikTok Shop" is not in the fixture — all rows should be zeros
        tiktok_records = [r for r in result if r.channel == "TikTok Shop" and r.sku != "Bundles"]
        for r in tiktok_records:
            assert r.units == 0
            assert r.revenue == 0.0

    def test_id_format(self, sales_df):
        """id must follow YYYYMMDD_Channel_SKU pattern (spaces → underscores)."""
        result = self._make_pipeline_with_df(sales_df)
        today = date.today().strftime("%Y%m%d")
        for r in result:
            assert r.id.startswith(today)
