"""
ADLS Gen2 Simulator

Simulates an Azure Data Lake Storage Gen2 container/folder structure using the
local filesystem. Manages bronze/silver/gold zones, tracks processed files, and
writes manifest files to mimic production ADLS behaviour.
"""

import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ZONES = ("landing", "bronze", "silver", "gold")


class ADLSSimulator:
    """
    Local filesystem simulation of ADLS Gen2.

    Directory layout mirrors a typical lakehouse container:
        <root>/
          landing/    -- raw files dropped by upstream producers
          bronze/     -- minimally processed parquet with metadata columns
          silver/     -- normalized, flattened tabular parquet
          gold/       -- business aggregations parquet + views
          _manifest/  -- internal tracking: processed files, run logs
    """

    def __init__(self, root_dir: str | Path):
        self.root = Path(root_dir)
        self._ensure_zone_dirs()
        self._manifest_dir = self.root / "_manifest"
        self._manifest_dir.mkdir(parents=True, exist_ok=True)
        self._processed_index_path = self._manifest_dir / "processed_files.json"
        self._processed_files: dict[str, dict] = self._load_processed_index()

    # ------------------------------------------------------------------
    # Zone path helpers
    # ------------------------------------------------------------------

    def zone_path(self, zone: str, *sub_paths: str) -> Path:
        if zone not in ZONES:
            raise ValueError(f"Unknown zone '{zone}'. Valid zones: {ZONES}")
        return self.root / zone / Path(*sub_paths) if sub_paths else self.root / zone

    def landing_path(self, *sub: str) -> Path:
        return self.zone_path("landing", *sub)

    def bronze_path(self, *sub: str) -> Path:
        return self.zone_path("bronze", *sub)

    def silver_path(self, *sub: str) -> Path:
        return self.zone_path("silver", *sub)

    def gold_path(self, *sub: str) -> Path:
        return self.zone_path("gold", *sub)

    # ------------------------------------------------------------------
    # File landing (simulates blob upload to ADLS)
    # ------------------------------------------------------------------

    def land_file(self, source_path: str | Path, sub_folder: str = "") -> Path:
        """Copy a source file into the landing zone."""
        source = Path(source_path)
        dest_dir = self.landing_path(sub_folder) if sub_folder else self.landing_path()
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / source.name
        shutil.copy2(source, dest)
        logger.debug("Landed file: %s -> %s", source, dest)
        return dest

    def land_directory(self, source_dir: str | Path, sub_folder: str = "") -> list[Path]:
        """Copy all files from source_dir into the landing zone."""
        source_dir = Path(source_dir)
        landed = []
        for f in sorted(source_dir.iterdir()):
            if f.is_file():
                landed.append(self.land_file(f, sub_folder))
        return landed

    # ------------------------------------------------------------------
    # Processed file tracking
    # ------------------------------------------------------------------

    def _load_processed_index(self) -> dict[str, dict]:
        if self._processed_index_path.exists():
            try:
                with open(self._processed_index_path) as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Processed index file is corrupt; starting fresh.")
        return {}

    def _save_processed_index(self) -> None:
        with open(self._processed_index_path, "w") as f:
            json.dump(self._processed_files, f, indent=2)

    def mark_file_processed(
        self,
        file_path: str | Path,
        pipeline_run_id: str = "",
        record_count: int = 0,
    ) -> None:
        """Record that a file has been successfully processed."""
        key = str(file_path)
        self._processed_files[key] = {
            "path": key,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": pipeline_run_id,
            "record_count": record_count,
        }
        self._save_processed_index()
        logger.debug("Marked as processed: %s", key)

    def is_file_processed(self, file_path: str | Path) -> bool:
        return str(file_path) in self._processed_files

    def get_unprocessed_landing_files(self, pattern: str = "*.json") -> list[Path]:
        """Return landing zone files that have not yet been processed."""
        landing = self.landing_path()
        all_files = sorted(landing.glob(pattern))
        pending = [f for f in all_files if not self.is_file_processed(f)]
        logger.info(
            "Landing zone: %d total files, %d unprocessed",
            len(all_files),
            len(pending),
        )
        return pending

    def get_processed_files(self) -> dict[str, dict]:
        return dict(self._processed_files)

    # ------------------------------------------------------------------
    # Manifest / run log
    # ------------------------------------------------------------------

    def write_run_manifest(self, run_id: str, manifest_data: dict[str, Any]) -> Path:
        """Write a JSON manifest file for a pipeline run."""
        manifest_path = self._manifest_dir / f"run_{run_id}.json"
        manifest_data["run_id"] = run_id
        manifest_data["written_at"] = datetime.now(timezone.utc).isoformat()
        with open(manifest_path, "w") as f:
            json.dump(manifest_data, f, indent=2)
        logger.debug("Run manifest written: %s", manifest_path)
        return manifest_path

    def list_run_manifests(self) -> list[Path]:
        return sorted(self._manifest_dir.glob("run_*.json"))

    def get_zone_stats(self) -> dict[str, Any]:
        """Return file counts and sizes per zone."""
        stats = {}
        for zone in ZONES:
            zone_dir = self.zone_path(zone)
            if zone_dir.exists():
                files = list(zone_dir.rglob("*"))
                file_list = [f for f in files if f.is_file()]
                total_bytes = sum(f.stat().st_size for f in file_list)
                stats[zone] = {
                    "file_count": len(file_list),
                    "total_size_bytes": total_bytes,
                    "total_size_mb": round(total_bytes / (1024 * 1024), 3),
                }
            else:
                stats[zone] = {"file_count": 0, "total_size_bytes": 0, "total_size_mb": 0}
        return stats

    # ------------------------------------------------------------------
    # Zone directory setup
    # ------------------------------------------------------------------

    def _ensure_zone_dirs(self) -> None:
        for zone in ZONES:
            (self.root / zone).mkdir(parents=True, exist_ok=True)
        logger.debug("ADLS zones initialised under %s", self.root)

    def cleanup_zone(self, zone: str) -> None:
        """Remove all files in a zone (useful for testing / full reloads)."""
        zone_dir = self.zone_path(zone)
        if zone_dir.exists():
            shutil.rmtree(zone_dir)
            zone_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Cleaned zone: %s", zone)
