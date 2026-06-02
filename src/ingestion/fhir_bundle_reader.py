"""
FHIR Bundle Reader

Reads FHIR R4 JSON bundles from a directory, extracts individual resources by
resourceType, and tracks ingestion metadata for downstream processing.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SUPPORTED_RESOURCE_TYPES = {"Patient", "Encounter", "Condition", "Observation", "Claim"}


class IngestionMetadata:
    """Tracks metadata for a single bundle ingestion."""

    def __init__(self, source_file: str, bundle_id: str | None = None):
        self.source_file = source_file
        self.bundle_id = bundle_id
        self.ingestion_timestamp = datetime.now(timezone.utc).isoformat()
        self.record_counts: dict[str, int] = {}
        self.total_entries: int = 0
        self.skipped_entries: int = 0
        self.errors: list[str] = []

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_file": self.source_file,
            "bundle_id": self.bundle_id,
            "ingestion_timestamp": self.ingestion_timestamp,
            "record_counts": self.record_counts,
            "total_entries": self.total_entries,
            "skipped_entries": self.skipped_entries,
            "errors": self.errors,
        }


class FHIRBundleReader:
    """
    Reads FHIR R4 bundles from a filesystem directory.

    Extracts resources grouped by resourceType and attaches ingestion metadata
    to each record for traceability across the pipeline.
    """

    def __init__(
        self,
        supported_resource_types: set[str] | None = None,
        bundle_entry_limit: int = 10000,
    ):
        self.supported_resource_types = supported_resource_types or SUPPORTED_RESOURCE_TYPES
        self.bundle_entry_limit = bundle_entry_limit

    def read_bundle_file(self, file_path: str | Path) -> tuple[dict[str, list[dict]], IngestionMetadata]:
        """
        Read a single FHIR bundle JSON file and extract resources by type.

        Returns:
            Tuple of (resources_by_type dict, IngestionMetadata)
        """
        file_path = Path(file_path)
        logger.info("Reading bundle file: %s", file_path)

        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        if raw.get("resourceType") != "Bundle":
            raise ValueError(f"File {file_path.name} is not a FHIR Bundle (got resourceType={raw.get('resourceType')})")

        bundle_id = raw.get("id")
        metadata = IngestionMetadata(source_file=str(file_path), bundle_id=bundle_id)

        entries = raw.get("entry", [])
        if len(entries) > self.bundle_entry_limit:
            logger.warning(
                "Bundle %s has %d entries, exceeding limit of %d. Truncating.",
                file_path.name,
                len(entries),
                self.bundle_entry_limit,
            )
            entries = entries[: self.bundle_entry_limit]

        metadata.total_entries = len(entries)
        resources_by_type: dict[str, list[dict]] = {rt: [] for rt in self.supported_resource_types}

        for idx, entry in enumerate(entries):
            resource = entry.get("resource")
            if not resource:
                metadata.skipped_entries += 1
                metadata.errors.append(f"Entry {idx} has no 'resource' field")
                continue

            resource_type = resource.get("resourceType")
            if resource_type not in self.supported_resource_types:
                metadata.skipped_entries += 1
                logger.debug("Skipping unsupported resource type: %s", resource_type)
                continue

            # Attach ingestion metadata directly to the resource dict
            resource["_ingestion_source_file"] = str(file_path)
            resource["_ingestion_timestamp"] = metadata.ingestion_timestamp
            resource["_bundle_id"] = bundle_id or ""
            resource["_full_url"] = entry.get("fullUrl", "")

            resources_by_type[resource_type].append(resource)
            metadata.record_counts[resource_type] = metadata.record_counts.get(resource_type, 0) + 1

        logger.info(
            "Ingested bundle %s: %s",
            file_path.name,
            ", ".join(f"{k}={v}" for k, v in metadata.record_counts.items()),
        )
        return resources_by_type, metadata

    def read_directory(
        self,
        directory: str | Path,
        file_pattern: str = "*.json",
        processed_files: set[str] | None = None,
    ) -> tuple[dict[str, list[dict]], list[IngestionMetadata]]:
        """
        Read all FHIR bundle files from a directory.

        Args:
            directory: Path to directory containing bundle JSON files
            file_pattern: Glob pattern to match files (default: *.json)
            processed_files: Set of already-processed file paths to skip (for incremental loads)

        Returns:
            Tuple of (merged resources_by_type, list of IngestionMetadata per file)
        """
        directory = Path(directory)
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        all_files = sorted(directory.glob(file_pattern))
        if not all_files:
            logger.warning("No files matching '%s' found in %s", file_pattern, directory)
            return {rt: [] for rt in self.supported_resource_types}, []

        if processed_files:
            pending = [f for f in all_files if str(f) not in processed_files]
            logger.info(
                "Incremental mode: %d/%d files pending processing",
                len(pending),
                len(all_files),
            )
            all_files = pending

        merged: dict[str, list[dict]] = {rt: [] for rt in self.supported_resource_types}
        all_metadata: list[IngestionMetadata] = []

        for file_path in all_files:
            try:
                resources, meta = self.read_bundle_file(file_path)
                for rt, records in resources.items():
                    merged[rt].extend(records)
                all_metadata.append(meta)
            except json.JSONDecodeError as exc:
                logger.error("Failed to parse JSON in %s: %s", file_path, exc)
            except ValueError as exc:
                logger.error("Invalid bundle in %s: %s", file_path, exc)
            except OSError as exc:
                logger.error("IO error reading %s: %s", file_path, exc)

        total_records = sum(len(v) for v in merged.values())
        logger.info(
            "Directory read complete: %d files, %d total resources",
            len(all_metadata),
            total_records,
        )
        return merged, all_metadata

    def get_summary(self, resources_by_type: dict[str, list[dict]]) -> dict[str, Any]:
        """Return a summary dict of ingested resource counts."""
        return {
            "resource_counts": {rt: len(records) for rt, records in resources_by_type.items()},
            "total_resources": sum(len(v) for v in resources_by_type.values()),
            "resource_types_present": [rt for rt, records in resources_by_type.items() if records],
        }
