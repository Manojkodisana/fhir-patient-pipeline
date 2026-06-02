"""
Bronze Layer Processor

Minimal transformation applied to raw FHIR resources:
  - Add pipeline metadata columns (ingestion_dt, source_file, pipeline_run_id)
  - Deduplicate by resource ID + meta.versionId
  - Serialize to Parquet in the bronze zone
  - Log record counts pre/post dedup
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class BronzeLayerProcessor:
    """
    Writes raw FHIR resources to the Bronze zone with minimal transformation.

    The primary concerns here are:
      1. Idempotency: dedup so re-running doesn't create duplicate rows
      2. Lineage: every row knows exactly where it came from
      3. Preserving the original JSON structure in a raw_json column
    """

    def __init__(self, bronze_zone_path: str | Path, pipeline_run_id: str | None = None):
        self.bronze_zone = Path(bronze_zone_path)
        self.pipeline_run_id = pipeline_run_id or str(uuid.uuid4())[:8]

    def process(
        self,
        resources: list[dict[str, Any]],
        resource_type: str,
        partition_date: str | None = None,
    ) -> tuple[Path, int, int]:
        """
        Process a batch of raw FHIR resources into the bronze zone.

        Args:
            resources: List of raw FHIR resource dicts (with _ingestion_* metadata)
            resource_type: FHIR resource type name (e.g., 'Patient')
            partition_date: Optional YYYY-MM-DD string for folder partitioning

        Returns:
            Tuple of (output_path, records_before_dedup, records_after_dedup)
        """
        if not resources:
            logger.info("Bronze [%s]: no records to process", resource_type)
            return self._output_path(resource_type, partition_date), 0, 0

        raw_count = len(resources)
        logger.info("Bronze [%s]: processing %d raw records", resource_type, raw_count)

        df = self._to_bronze_dataframe(resources, resource_type)
        before_dedup = len(df)
        df = self._deduplicate(df, resource_type)
        after_dedup = len(df)

        if before_dedup != after_dedup:
            logger.warning(
                "Bronze [%s]: removed %d duplicate records (before=%d, after=%d)",
                resource_type,
                before_dedup - after_dedup,
                before_dedup,
                after_dedup,
            )

        output_path = self._output_path(resource_type, partition_date)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_parquet(df, output_path)

        logger.info(
            "Bronze [%s]: wrote %d records to %s",
            resource_type,
            after_dedup,
            output_path,
        )
        return output_path, before_dedup, after_dedup

    def process_all(
        self,
        resources_by_type: dict[str, list[dict[str, Any]]],
        partition_date: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Process all resource types from a bundle ingestion.

        Returns a dict keyed by resource_type with stats for each.
        """
        if partition_date is None:
            partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        results = {}
        for resource_type, records in resources_by_type.items():
            if not records:
                continue
            path, before, after = self.process(records, resource_type, partition_date)
            results[resource_type] = {
                "output_path": str(path),
                "records_ingested": before,
                "records_written": after,
                "duplicates_removed": before - after,
                "partition_date": partition_date,
            }
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_bronze_dataframe(
        self, resources: list[dict[str, Any]], resource_type: str
    ) -> pd.DataFrame:
        """Convert raw FHIR resources to a bronze DataFrame."""
        now_utc = datetime.now(timezone.utc).isoformat()
        rows = []
        for res in resources:
            row = {
                "resource_id": res.get("id", ""),
                "resource_type": resource_type,
                "version_id": (res.get("meta") or {}).get("versionId", "1"),
                "last_updated": (res.get("meta") or {}).get("lastUpdated", ""),
                "source_file": res.get("_ingestion_source_file", ""),
                "bundle_id": res.get("_bundle_id", ""),
                "full_url": res.get("_full_url", ""),
                "ingestion_timestamp": res.get("_ingestion_timestamp", now_utc),
                "ingestion_date": res.get("_ingestion_timestamp", now_utc)[:10],
                "pipeline_run_id": self.pipeline_run_id,
                # Persist full JSON for downstream parsing flexibility
                "raw_json": json.dumps(
                    {k: v for k, v in res.items() if not k.startswith("_")},
                    default=str,
                ),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        # Ensure consistent column types
        str_cols = [c for c in df.columns if c != "raw_json"]
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip()
        return df

    def _deduplicate(self, df: pd.DataFrame, resource_type: str) -> pd.DataFrame:
        """
        Deduplicate by (resource_id, version_id).
        When duplicates exist, keep the row with the latest ingestion_timestamp.
        """
        dedup_cols = ["resource_id", "version_id"]
        df_sorted = df.sort_values("ingestion_timestamp", ascending=False)
        deduped = df_sorted.drop_duplicates(subset=dedup_cols, keep="first")
        return deduped.reset_index(drop=True)

    def _output_path(self, resource_type: str, partition_date: str | None) -> Path:
        """Build the output parquet file path."""
        rt_lower = resource_type.lower()
        if partition_date:
            return self.bronze_zone / rt_lower / f"dt={partition_date}" / f"{rt_lower}.parquet"
        return self.bronze_zone / rt_lower / f"{rt_lower}.parquet"

    def _write_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
        """Write DataFrame to parquet with snappy compression."""
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(output_path), compression="snappy")

    def read_bronze(self, resource_type: str, partition_date: str | None = None) -> pd.DataFrame:
        """Read a bronze parquet file back into a DataFrame."""
        path = self._output_path(resource_type, partition_date)
        if not path.exists():
            # Try scanning all partitions
            base = self.bronze_zone / resource_type.lower()
            if base.exists():
                all_files = list(base.rglob("*.parquet"))
                if all_files:
                    return pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
            logger.warning("No bronze data found for %s", resource_type)
            return pd.DataFrame()
        return pd.read_parquet(path)
