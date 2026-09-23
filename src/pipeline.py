import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
import pandas as pd

from src import settings, data_handler
from src.reporting import publisher
from src.schemas import ExtractResult

logger = logging.getLogger(__name__)


class DataPipeline(ABC):
    """
    Abstract base class for data pipelines (Sales, Inventory, etc.).
    Follows an Extract -> Transform -> Load (ETL) pattern.
    """

    def __init__(
        self,
        report_type: str,
        channels: Optional[list[str]] = None,
        test_mode: bool = False,
        force_publish: bool = False,
    ):
        self.report_type = report_type
        # Use provided channels or default to settings.CHANNEL_ORDER (Inventory default)
        self.channels = channels if channels is not None else settings.CHANNEL_ORDER
        self.test_mode = test_mode
        self.force_publish = force_publish
        # Status summary tracks the data date for each channel
        self.status_summary = {ch: None for ch in self.channels}
        # Populated by subclass extract() with the .name of each input file used
        self.source_files: list[str] = []

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
        Saves data to disk, publishes to the workbooks/Teams, and (optionally)
        posts to the legacy n8n webhook.
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
            data_handler.log_run_history(validated_data, self.report_type, self.source_files)
        else:
            logger.warning("No data to save to disk.")

        # 3. Publish: history + snapshot workbooks, deltas, Teams reports.
        #    This is the internal replacement for the n8n `update-sku-data` workflow.
        if settings.PUBLISH_ENABLED:
            self._publish(validated_data)

        # 4. Legacy n8n webhook — off unless WEBHOOK_ENABLED=true.
        if not self.test_mode and settings.WEBHOOK_ENABLED:
            data_handler.post_to_webhook(
                validated_data=validated_data,
                metadata=self.status_summary,
                report_type=self.report_type,
            )
        elif settings.WEBHOOK_ENABLED:
            logger.info("🧪 Test Mode: Skipping webhook post.")
        else:
            logger.info("🔌 n8n webhook lane disabled (WEBHOOK_ENABLED=false).")

    def _publish(self, validated_data: list[Any]):
        """Run the internal publish; test mode still builds the reports (dry run)."""
        try:
            result = publisher.publish(
                report_type=self.report_type,
                validated_data=validated_data,
                status_summary=self.status_summary,
                dry_run=self.test_mode,
                force=self.force_publish,
            )
        except Exception as e:
            logger.error(f"❌ Publish failed: {e}", exc_info=True)
            return

        if result.skipped_reason:
            logger.info(f"⏭️  Publish skipped: {result.skipped_reason}")
        elif result.errors:
            logger.warning(f"⚠️  Publish finished with {len(result.errors)} error(s).")
