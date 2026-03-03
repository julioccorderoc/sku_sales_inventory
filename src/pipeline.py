import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
import pandas as pd

from src import settings, data_handler
from src.schemas import ExtractResult

logger = logging.getLogger(__name__)


class DataPipeline(ABC):
    """
    Abstract base class for data pipelines (Sales, Inventory, etc.).
    Follows an Extract -> Transform -> Load (ETL) pattern.
    """

    def __init__(self, report_type: str, channels: Optional[list[str]] = None, test_mode: bool = False):
        self.report_type = report_type
        # Use provided channels or default to settings.CHANNEL_ORDER (Inventory default)
        self.channels = channels if channels is not None else settings.CHANNEL_ORDER
        self.test_mode = test_mode
        # Status summary tracks the data date for each channel
        self.status_summary = {ch: None for ch in self.channels}

    def run(self):
        """
        Orchestrates the pipeline execution.
        """
        logger.info(f"🚀 STEP: {self.report_type.upper()} REPORT")
        logger.info("-" * 30)

        # --- 1. EXTRACT ---
        extract_result = self.extract()
        raw_data = extract_result.df
        if raw_data is None or raw_data.empty:
            logger.warning(f"⚠️ No data extracted for {self.report_type}. Sending empty status matches.")
            self.load([])
            return

        # --- 2. TRANSFORM ---
        # Transform returns a list of Pydantic models (validated data)
        validated_data = self.transform(raw_data, extract_result.bundle_rows)
        if validated_data is None:
            logger.error(f"❌ Transformation failed for {self.report_type}.")
            return

        # --- 3. LOAD ---
        self.load(validated_data)

        logger.info(f"✅ {self.report_type.capitalize()} Pipeline Finished.\n")
        logger.info("=" * 60)

    @abstractmethod
    def extract(self) -> ExtractResult:
        """
        Responsible for finding files, running parsers, and returning an ExtractResult
        containing the combined raw DataFrame and any bundle_rows.
        Should also populate self.status_summary as it processes sources.
        """
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame, bundle_rows: list[dict]) -> list[Any] | None:
        """
        Responsible for normalization (zero-filling), ID generation, and validation.
        Returns a list of validated Pydantic models.
        bundle_rows carries bundle data from the extract phase for sales pipelines.
        """
        pass

    def load(self, validated_data: list[Any]):
        """
        Saves data to disk and posts to webhook.
        """
        # 1. Print Status Summary
        if self.channels:
            logger.info("\n--- Final Status Summary ---")
            for ch in self.channels:
                date_val = self.status_summary.get(ch)
                logger.info(f"{ch}: {date_val.isoformat() if date_val else 'No data'}")

        # 2. Save Code Outputs (CSV/JSON)
        if validated_data:
            data_handler.save_outputs(validated_data, f"{self.report_type}_report")
        else:
            logger.warning("No data to save to disk.")

        # 3. Post to Webhook
        if not self.test_mode:
            data_handler.post_to_webhook(
                validated_data=validated_data,
                metadata=self.status_summary,
                report_type=self.report_type,
            )
        else:
            logger.info("🧪 Test Mode: Skipping webhook post.")
