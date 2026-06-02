"""
Tests for src/pipeline/orchestrator.py

Covers:
  - Retry logic with exponential backoff
  - StageResult tracking
  - Pipeline run produces JSON log
  - Full pipeline run on sample data (integration test)
"""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import tempfile

import pytest

from src.pipeline.orchestrator import (
    PipelineOrchestrator,
    PipelineStageError,
    StageResult,
    _with_retry,
)


# ---------------------------------------------------------------------------
# _with_retry tests
# ---------------------------------------------------------------------------

def test_retry_succeeds_on_first_attempt():
    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        return "success"

    result = _with_retry(fn, "test_stage", max_attempts=3, backoff_base=0.01)
    assert result == "success"
    assert call_count == 1


def test_retry_succeeds_on_second_attempt():
    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("Temporary failure")
        return "recovered"

    result = _with_retry(fn, "test_stage", max_attempts=3, backoff_base=0.01)
    assert result == "recovered"
    assert call_count == 2


def test_retry_raises_after_max_attempts():
    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        raise RuntimeError(f"Persistent failure #{call_count}")

    with pytest.raises(PipelineStageError) as exc_info:
        _with_retry(fn, "always_fails", max_attempts=3, backoff_base=0.01)

    assert call_count == 3
    assert "always_fails" in str(exc_info.value)


def test_retry_with_zero_extra_attempts_fails_immediately():
    """max_attempts=1 means no retries, just one try."""
    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        raise ValueError("no retry")

    with pytest.raises(PipelineStageError):
        _with_retry(fn, "stage", max_attempts=1, backoff_base=0.01)

    assert call_count == 1


def test_retry_backoff_increases():
    """Verify sleep durations approximately follow exponential backoff pattern."""
    sleep_calls = []
    original_sleep = time.sleep

    def mock_sleep(duration):
        sleep_calls.append(duration)

    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("fail")
        return "done"

    with patch("time.sleep", side_effect=mock_sleep):
        _with_retry(fn, "stage", max_attempts=3, backoff_base=1.0, backoff_multiplier=2.0)

    # First sleep: 1.0 * 2^0 = 1.0, second sleep: 1.0 * 2^1 = 2.0
    assert len(sleep_calls) == 2
    assert sleep_calls[0] == pytest.approx(1.0)
    assert sleep_calls[1] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# StageResult tests
# ---------------------------------------------------------------------------

def test_stage_result_defaults():
    result = StageResult("ingest")
    assert result.stage == "ingest"
    assert result.status == "pending"
    assert result.records_processed == 0
    assert result.error == ""


def test_stage_result_to_dict_has_required_keys():
    result = StageResult("bronze")
    result.status = "SUCCESS"
    result.records_processed = 42
    d = result.to_dict()

    assert d["stage"] == "bronze"
    assert d["status"] == "SUCCESS"
    assert d["records_processed"] == 42
    assert "start_time" in d
    assert "end_time" in d
    assert "duration_seconds" in d


# ---------------------------------------------------------------------------
# Full pipeline run (integration)
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_pipeline_dir(tmp_path):
    """Set up a minimal pipeline directory with config and sample data."""
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)

    # Copy one sample bundle
    sample_bundle = Path("data/raw/sample_bundle_001.json")
    if sample_bundle.exists():
        import shutil
        shutil.copy(sample_bundle, raw_dir / "sample_bundle_001.json")
    else:
        # Create a minimal synthetic bundle
        bundle = {
            "resourceType": "Bundle",
            "id": "test-bundle",
            "type": "collection",
            "entry": [
                {
                    "fullUrl": "urn:uuid:test-patient",
                    "resource": {
                        "resourceType": "Patient",
                        "id": "test-patient",
                        "name": [{"family": "Test", "given": ["User"]}],
                        "gender": "male",
                        "birthDate": "1990-01-01",
                    },
                },
                {
                    "fullUrl": "urn:uuid:test-enc",
                    "resource": {
                        "resourceType": "Encounter",
                        "id": "test-enc",
                        "status": "finished",
                        "subject": {"reference": "Patient/test-patient"},
                        "period": {
                            "start": "2023-01-10T09:00:00Z",
                            "end": "2023-01-10T10:30:00Z",
                        },
                    },
                },
                {
                    "fullUrl": "urn:uuid:test-claim",
                    "resource": {
                        "resourceType": "Claim",
                        "id": "test-claim",
                        "status": "active",
                        "patient": {"reference": "Patient/test-patient"},
                        "total": {"value": 200.0, "currency": "USD"},
                    },
                },
            ],
        }
        with open(raw_dir / "test_bundle.json", "w") as f:
            json.dump(bundle, f)

    # Write minimal pipeline config
    config = {
        "pipeline": {"name": "test-pipeline", "version": "0.1.0"},
        "paths": {
            "raw_data_dir": str(raw_dir),
            "landing_zone": str(tmp_path / "data" / "landing"),
            "bronze_zone": str(tmp_path / "data" / "bronze"),
            "silver_zone": str(tmp_path / "data" / "silver"),
            "gold_zone": str(tmp_path / "data" / "gold"),
            "warehouse_db": str(tmp_path / "data" / "warehouse" / "test.duckdb"),
            "watermark_file": str(tmp_path / "data" / "watermarks.json"),
            "pipeline_log_dir": str(tmp_path / "data" / "logs"),
        },
        "ingestion": {
            "supported_resource_types": ["Patient", "Encounter", "Condition", "Observation", "Claim"],
            "bundle_entry_limit": 10000,
            "file_pattern": "*.json",
        },
        "validation": {
            "strict_mode": False,
            "min_quality_score": 60,
        },
        "transformation": {
            "bronze": {"dedup_fields": {}},
            "silver": {},
            "gold": {},
        },
        "warehouse": {"duckdb": {"memory_limit": "512MB", "threads": 1}},
        "retry": {"max_attempts": 1, "backoff_base_seconds": 0, "backoff_multiplier": 1},
        "logging": {"level": "WARNING"},
    }
    import yaml
    config_path = tmp_path / "config" / "pipeline_config.yaml"
    config_path.parent.mkdir(parents=True)
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return tmp_path, config_path


def test_full_pipeline_run_succeeds(tmp_pipeline_dir):
    tmp_path, config_path = tmp_pipeline_dir
    orchestrator = PipelineOrchestrator(config_path=config_path)
    summary = orchestrator.run()

    assert summary["status"] == "SUCCESS"
    assert summary["run_id"] is not None
    assert len(summary["stages"]) == 6
    stage_names = [s["stage"] for s in summary["stages"]]
    assert "ingest" in stage_names
    assert "validate" in stage_names
    assert "bronze" in stage_names
    assert "silver" in stage_names
    assert "gold" in stage_names
    assert "warehouse" in stage_names


def test_full_pipeline_writes_run_log(tmp_pipeline_dir):
    tmp_path, config_path = tmp_pipeline_dir
    orchestrator = PipelineOrchestrator(config_path=config_path)
    summary = orchestrator.run()

    run_id = summary["run_id"]
    log_dir = tmp_path / "data" / "logs"
    log_files = list(log_dir.glob(f"run_{run_id}.json"))
    assert len(log_files) == 1

    with open(log_files[0]) as f:
        log_data = json.load(f)
    assert log_data["run_id"] == run_id
    assert log_data["status"] == "SUCCESS"


def test_pipeline_stage_results_have_durations(tmp_pipeline_dir):
    tmp_path, config_path = tmp_pipeline_dir
    orchestrator = PipelineOrchestrator(config_path=config_path)
    summary = orchestrator.run()

    for stage in summary["stages"]:
        assert stage["duration_seconds"] >= 0
        assert stage["start_time"] != ""
        assert stage["end_time"] != ""


def test_pipeline_ingest_stage_records_count(tmp_pipeline_dir):
    tmp_path, config_path = tmp_pipeline_dir
    orchestrator = PipelineOrchestrator(config_path=config_path)
    summary = orchestrator.run()

    ingest_stage = next(s for s in summary["stages"] if s["stage"] == "ingest")
    assert ingest_stage["records_processed"] > 0
    assert ingest_stage["status"] == "SUCCESS"
