"""
Pipeline Orchestrator

Main entry point for the FHIR patient pipeline. Coordinates all stages:
  ingest → validate → bronze → silver → gold → warehouse load

Includes retry logic with exponential backoff and pipeline run tracking.
Writes a JSON run log on completion.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from src.ingestion.fhir_bundle_reader import FHIRBundleReader
from src.ingestion.adls_simulator import ADLSSimulator
from src.validation.fhir_validator import FHIRValidator
from src.validation.data_quality_checks import DataQualityEngine
from src.transformation.bronze_layer import BronzeLayerProcessor
from src.transformation.silver_layer import SilverLayerProcessor
from src.transformation.gold_layer import GoldLayerProcessor
from src.warehouse.duckdb_loader import DuckDBLoader

logger = logging.getLogger(__name__)


class PipelineStageError(Exception):
    """Raised when a pipeline stage fails after all retries."""
    pass


def _with_retry(
    fn: Callable,
    stage_name: str,
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    backoff_multiplier: float = 2.0,
) -> Any:
    """
    Execute fn() with exponential backoff retries.
    Raises PipelineStageError after max_attempts failures.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                wait = backoff_base * (backoff_multiplier ** (attempt - 1))
                logger.warning(
                    "Stage '%s' attempt %d/%d failed: %s. Retrying in %.1fs...",
                    stage_name,
                    attempt,
                    max_attempts,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "Stage '%s' failed after %d attempts: %s",
                    stage_name,
                    max_attempts,
                    exc,
                )
    raise PipelineStageError(f"Stage '{stage_name}' failed: {last_exc}") from last_exc


class StageResult:
    def __init__(self, stage: str):
        self.stage = stage
        self.status = "pending"
        self.records_processed = 0
        self.start_time: str = ""
        self.end_time: str = ""
        self.duration_seconds: float = 0.0
        self.details: dict = {}
        self.error: str = ""

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "status": self.status,
            "records_processed": self.records_processed,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(self.duration_seconds, 3),
            "details": self.details,
            "error": self.error,
        }


class PipelineOrchestrator:
    """
    Orchestrates the full FHIR pipeline run.

    Stages:
      1. Ingest    - Read FHIR bundles from raw data directory
      2. Validate  - Schema validation + data quality checks
      3. Bronze    - Write raw+metadata parquet to bronze zone
      4. Silver    - Flatten and normalize to silver zone
      5. Gold      - Build business aggregations in gold zone
      6. Warehouse - Load all zones into DuckDB
    """

    def __init__(self, config_path: str | Path = "config/pipeline_config.yaml"):
        self.config = self._load_config(config_path)
        self.run_id = str(uuid.uuid4())[:8]
        self.stage_results: list[StageResult] = []
        self._setup_logging()

        paths = self.config.get("paths", {})
        self.raw_data_dir = Path(paths.get("raw_data_dir", "data/raw"))
        self.bronze_zone = Path(paths.get("bronze_zone", "data/bronze"))
        self.silver_zone = Path(paths.get("silver_zone", "data/silver"))
        self.gold_zone = Path(paths.get("gold_zone", "data/gold"))
        self.warehouse_db = Path(paths.get("warehouse_db", "data/warehouse/fhir_warehouse.duckdb"))
        self.log_dir = Path(paths.get("pipeline_log_dir", "data/logs"))
        self.adls_root = Path(paths.get("landing_zone", "data/landing")).parent

        retry_cfg = self.config.get("retry", {})
        self.max_retries = retry_cfg.get("max_attempts", 3)
        self.backoff_base = retry_cfg.get("backoff_base_seconds", 2)
        self.backoff_mult = retry_cfg.get("backoff_multiplier", 2)

        val_cfg = self.config.get("validation", {})
        self.min_quality_score = val_cfg.get("min_quality_score", 70)

    def run(self) -> dict[str, Any]:
        """Execute the full pipeline. Returns the run summary dict."""
        run_start = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("Pipeline run %s starting at %s", self.run_id, run_start.isoformat())
        logger.info("=" * 60)

        pipeline_status = "SUCCESS"
        pipeline_error = ""
        resources_by_type: dict[str, list] = {}

        try:
            # Stage 1: Ingest
            resources_by_type = self._run_stage("ingest", lambda: self._stage_ingest())

            # Stage 2: Validate
            resources_by_type = self._run_stage("validate", lambda: self._stage_validate(resources_by_type))

            # Stage 3: Bronze
            self._run_stage("bronze", lambda: self._stage_bronze(resources_by_type))

            # Stage 4: Silver
            self._run_stage("silver", lambda: self._stage_silver())

            # Stage 5: Gold
            self._run_stage("gold", lambda: self._stage_gold())

            # Stage 6: Warehouse
            self._run_stage("warehouse", lambda: self._stage_warehouse())

        except PipelineStageError as exc:
            pipeline_status = "FAILED"
            pipeline_error = str(exc)
            logger.error("Pipeline FAILED: %s", exc)

        run_end = datetime.now(timezone.utc)
        total_duration = (run_end - run_start).total_seconds()

        summary = {
            "run_id": self.run_id,
            "status": pipeline_status,
            "start_time": run_start.isoformat(),
            "end_time": run_end.isoformat(),
            "duration_seconds": round(total_duration, 2),
            "error": pipeline_error,
            "stages": [r.to_dict() for r in self.stage_results],
            "total_records_processed": sum(r.records_processed for r in self.stage_results),
        }

        self._write_run_log(summary)
        logger.info("Pipeline run %s complete. Status=%s Duration=%.1fs", self.run_id, pipeline_status, total_duration)
        return summary

    # ------------------------------------------------------------------
    # Stage implementations
    # ------------------------------------------------------------------

    def _stage_ingest(self) -> dict[str, list]:
        reader = FHIRBundleReader(
            supported_resource_types=set(
                self.config.get("ingestion", {}).get("supported_resource_types",
                ["Patient", "Encounter", "Condition", "Observation", "Claim"])
            ),
            bundle_entry_limit=self.config.get("ingestion", {}).get("bundle_entry_limit", 10000),
        )
        pattern = self.config.get("ingestion", {}).get("file_pattern", "*.json")
        resources_by_type, meta_list = reader.read_directory(self.raw_data_dir, file_pattern=pattern)

        total = sum(len(v) for v in resources_by_type.values())
        self._update_stage_records("ingest", total)
        self._update_stage_details("ingest", {
            "files_read": len(meta_list),
            "resource_counts": {rt: len(v) for rt, v in resources_by_type.items()},
        })
        return resources_by_type

    def _stage_validate(self, resources_by_type: dict) -> dict:
        validator = FHIRValidator(strict_mode=self.config.get("validation", {}).get("strict_mode", False))
        quality_engine = DataQualityEngine(min_quality_score=self.min_quality_score)

        validation_summary = {}
        quality_summary = {}
        total_checked = 0
        total_errors = 0

        for rt, resources in resources_by_type.items():
            if not resources:
                continue

            results = validator.validate_batch(resources)
            errors = sum(len(r.errors) for r in results)
            total_checked += len(results)
            total_errors += errors

            validation_summary[rt] = {
                "checked": len(results),
                "valid": sum(1 for r in results if r.is_valid),
                "errors": errors,
                "warnings": sum(len(r.warnings) for r in results),
            }

            quality_report = quality_engine.run_checks(resources, rt)
            quality_summary[rt] = {
                "score": quality_report.quality_score,
                "passed": quality_report.passed_pipeline_threshold,
            }

            if not quality_report.passed_pipeline_threshold:
                logger.warning(
                    "Quality score for %s is %.1f (below threshold %.1f)",
                    rt,
                    quality_report.quality_score,
                    self.min_quality_score,
                )

        # Cross-resource reference check
        ref_issues = validator.validate_references(resources_by_type)
        if ref_issues:
            logger.warning("%d cross-reference issues found", len(ref_issues))

        self._update_stage_records("validate", total_checked)
        self._update_stage_details("validate", {
            "validation": validation_summary,
            "quality": quality_summary,
            "reference_issues": len(ref_issues),
            "total_errors": total_errors,
        })

        # Filter out records that failed validation (only ERROR severity)
        if total_errors > 0:
            logger.warning("Validation found %d errors across all resources", total_errors)

        return resources_by_type

    def _stage_bronze(self, resources_by_type: dict) -> None:
        processor = BronzeLayerProcessor(
            bronze_zone_path=self.bronze_zone,
            pipeline_run_id=self.run_id,
        )
        partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        results = processor.process_all(resources_by_type, partition_date=partition_date)

        total_written = sum(r.get("records_written", 0) for r in results.values())
        self._update_stage_records("bronze", total_written)
        self._update_stage_details("bronze", results)

    def _stage_silver(self) -> None:
        from src.transformation.bronze_layer import BronzeLayerProcessor
        bronze_proc = BronzeLayerProcessor(bronze_zone_path=self.bronze_zone)
        silver_proc = SilverLayerProcessor(silver_zone_path=self.silver_zone)

        resource_types = ["Patient", "Encounter", "Condition", "Observation", "Claim"]
        total_written = 0
        details = {}

        for rt in resource_types:
            bronze_df = bronze_proc.read_bronze(rt)
            if bronze_df.empty:
                logger.debug("Silver [%s]: no bronze data, skipping", rt)
                continue
            silver_df = silver_proc.process_resource_type(rt, bronze_df)
            silver_proc.write_silver(rt, silver_df)
            total_written += len(silver_df)
            details[rt] = len(silver_df)

        self._update_stage_records("silver", total_written)
        self._update_stage_details("silver", details)

    def _stage_gold(self) -> None:
        import pandas as pd
        silver_proc = SilverLayerProcessor(silver_zone_path=self.silver_zone)
        gold_proc = GoldLayerProcessor(gold_zone_path=self.gold_zone)

        patients = silver_proc.read_silver("Patient")
        encounters = silver_proc.read_silver("Encounter")
        conditions = silver_proc.read_silver("Condition")
        observations = silver_proc.read_silver("Observation")
        claims = silver_proc.read_silver("Claim")

        output_paths = gold_proc.build_all(patients, encounters, conditions, observations, claims)
        total_gold_rows = 0
        for table_name in output_paths:
            df = gold_proc.read_gold(table_name)
            total_gold_rows += len(df)

        self._update_stage_records("gold", total_gold_rows)
        self._update_stage_details("gold", output_paths)

    def _stage_warehouse(self) -> None:
        db_cfg = self.config.get("warehouse", {}).get("duckdb", {})
        loader = DuckDBLoader(
            db_path=self.warehouse_db,
            memory_limit=db_cfg.get("memory_limit", "2GB"),
            threads=db_cfg.get("threads", 4),
        )
        try:
            loader.initialize_schema()
            bronze_counts = loader.load_bronze(self.bronze_zone)
            silver_counts = loader.load_silver(self.silver_zone)
            gold_counts = loader.load_gold(self.gold_zone)
            loader.create_views()
            sanity = loader.run_sanity_checks()

            total_rows = sum(bronze_counts.values()) + sum(silver_counts.values()) + sum(gold_counts.values())
            self._update_stage_records("warehouse", total_rows)
            self._update_stage_details("warehouse", {
                "bronze_tables": bronze_counts,
                "silver_tables": silver_counts,
                "gold_tables": gold_counts,
                "sanity_checks": {k: v for k, v in sanity.items() if not isinstance(v, dict)},
            })
        finally:
            loader.close()

    # ------------------------------------------------------------------
    # Stage runner with retry
    # ------------------------------------------------------------------

    def _run_stage(self, stage_name: str, fn: Callable) -> Any:
        result = StageResult(stage_name)
        result.start_time = datetime.now(timezone.utc).isoformat()
        self.stage_results.append(result)
        t_start = time.monotonic()

        try:
            output = _with_retry(
                fn,
                stage_name=stage_name,
                max_attempts=self.max_retries,
                backoff_base=self.backoff_base,
                backoff_multiplier=self.backoff_mult,
            )
            result.status = "SUCCESS"
            return output
        except PipelineStageError as exc:
            result.status = "FAILED"
            result.error = str(exc)
            raise
        finally:
            result.end_time = datetime.now(timezone.utc).isoformat()
            result.duration_seconds = time.monotonic() - t_start

    def _update_stage_records(self, stage_name: str, count: int) -> None:
        for r in self.stage_results:
            if r.stage == stage_name:
                r.records_processed = count
                return

    def _update_stage_details(self, stage_name: str, details: dict) -> None:
        for r in self.stage_results:
            if r.stage == stage_name:
                r.details = details
                return

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _load_config(self, config_path: str | Path) -> dict:
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                return yaml.safe_load(f)
        logger.warning("Config file not found: %s. Using defaults.", config_path)
        return {}

    def _setup_logging(self) -> None:
        log_cfg = self.config.get("logging", {})
        level = getattr(logging, log_cfg.get("level", "INFO"), logging.INFO)
        fmt = log_cfg.get("format", "%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        logging.basicConfig(level=level, format=fmt, force=True)

    def _write_run_log(self, summary: dict) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / f"run_{self.run_id}.json"
        with open(log_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info("Run log written: %s", log_path)


def main():
    """CLI entry point."""
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/pipeline_config.yaml"
    orchestrator = PipelineOrchestrator(config_path=config_path)
    summary = orchestrator.run()
    print(json.dumps(summary, indent=2, default=str))
    sys.exit(0 if summary["status"] == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
