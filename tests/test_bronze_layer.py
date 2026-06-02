"""
Tests for src/transformation/bronze_layer.py

Covers:
  - DataFrame construction from raw FHIR resources
  - Deduplication by (resource_id, version_id)
  - Metadata column attachment
  - Parquet write/read round-trip
  - process_all for multiple resource types
"""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.transformation.bronze_layer import BronzeLayerProcessor


def make_resource(
    resource_type: str,
    resource_id: str,
    version_id: str = "1",
    source_file: str = "test_bundle.json",
) -> dict:
    return {
        "resourceType": resource_type,
        "id": resource_id,
        "meta": {"versionId": version_id, "lastUpdated": "2024-01-01T00:00:00Z"},
        "_ingestion_source_file": source_file,
        "_ingestion_timestamp": "2024-01-15T08:00:00+00:00",
        "_bundle_id": "bundle-test",
        "_full_url": f"urn:uuid:{resource_id}",
    }


@pytest.fixture
def tmp_bronze(tmp_path):
    return tmp_path / "bronze"


@pytest.fixture
def processor(tmp_bronze):
    return BronzeLayerProcessor(bronze_zone_path=tmp_bronze, pipeline_run_id="test-run-001")


def test_process_creates_parquet_file(processor, tmp_bronze):
    resources = [make_resource("Patient", "p-001")]
    path, before, after = processor.process(resources, "Patient", partition_date="2024-01-15")

    assert path.exists()
    assert path.suffix == ".parquet"
    assert before == 1
    assert after == 1


def test_bronze_dataframe_has_metadata_columns(processor):
    resources = [make_resource("Patient", "p-001")]
    df = processor._to_bronze_dataframe(resources, "Patient")

    assert "resource_id" in df.columns
    assert "resource_type" in df.columns
    assert "source_file" in df.columns
    assert "pipeline_run_id" in df.columns
    assert "ingestion_timestamp" in df.columns
    assert "raw_json" in df.columns


def test_bronze_dataframe_raw_json_is_valid(processor):
    resources = [make_resource("Patient", "p-001")]
    df = processor._to_bronze_dataframe(resources, "Patient")
    raw = json.loads(df.loc[0, "raw_json"])
    assert raw["resourceType"] == "Patient"
    assert raw["id"] == "p-001"
    # Internal metadata keys should NOT be in raw_json
    assert "_ingestion_source_file" not in raw


def test_deduplication_removes_exact_duplicates(processor):
    resources = [
        make_resource("Patient", "p-001", version_id="1"),
        make_resource("Patient", "p-001", version_id="1"),  # exact duplicate
        make_resource("Patient", "p-002", version_id="1"),
    ]
    df = processor._to_bronze_dataframe(resources, "Patient")
    deduped = processor._deduplicate(df, "Patient")
    assert len(deduped) == 2
    ids = list(deduped["resource_id"])
    assert "p-001" in ids
    assert "p-002" in ids


def test_deduplication_keeps_different_versions(processor):
    resources = [
        make_resource("Patient", "p-001", version_id="1"),
        make_resource("Patient", "p-001", version_id="2"),  # new version - should keep
    ]
    df = processor._to_bronze_dataframe(resources, "Patient")
    deduped = processor._deduplicate(df, "Patient")
    # Different version_ids -> kept separately? No: dedup key is (id, version_id), both kept
    assert len(deduped) == 2


def test_deduplication_returns_latest_when_same_id_version(processor, tmp_bronze):
    """When same (id, version_id) appear twice, the one with later ingestion_timestamp wins."""
    resources = [
        {**make_resource("Patient", "p-dup"), "_ingestion_timestamp": "2024-01-01T06:00:00+00:00"},
        {**make_resource("Patient", "p-dup"), "_ingestion_timestamp": "2024-01-01T12:00:00+00:00"},
    ]
    df = processor._to_bronze_dataframe(resources, "Patient")
    deduped = processor._deduplicate(df, "Patient")
    assert len(deduped) == 1
    assert deduped.iloc[0]["ingestion_timestamp"] == "2024-01-01T12:00:00+00:00"


def test_empty_resource_list_returns_zero_counts(processor, tmp_bronze):
    path, before, after = processor.process([], "Patient", partition_date="2024-01-15")
    assert before == 0
    assert after == 0


def test_parquet_round_trip(processor, tmp_bronze):
    resources = [
        make_resource("Patient", "p-001"),
        make_resource("Patient", "p-002"),
    ]
    processor.process(resources, "Patient", partition_date="2024-01-15")
    read_back = processor.read_bronze("Patient", partition_date="2024-01-15")

    assert len(read_back) == 2
    assert set(read_back["resource_id"]) == {"p-001", "p-002"}


def test_process_all_handles_multiple_types(processor, tmp_bronze):
    resources_by_type = {
        "Patient": [make_resource("Patient", "p-001")],
        "Encounter": [
            make_resource("Encounter", "enc-001"),
            make_resource("Encounter", "enc-002"),
        ],
        "Condition": [],  # empty - should be skipped
        "Observation": [make_resource("Observation", "obs-001")],
        "Claim": [],
    }
    results = processor.process_all(resources_by_type, partition_date="2024-01-15")

    assert "Patient" in results
    assert "Encounter" in results
    assert results["Patient"]["records_written"] == 1
    assert results["Encounter"]["records_written"] == 2
    assert "Observation" in results
    assert "Condition" not in results  # empty was skipped


def test_pipeline_run_id_attached_to_rows(processor):
    resources = [make_resource("Patient", "p-001")]
    df = processor._to_bronze_dataframe(resources, "Patient")
    assert df.iloc[0]["pipeline_run_id"] == "test-run-001"
