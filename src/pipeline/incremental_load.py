"""
Incremental Load Manager

Implements watermark-based incremental loading:
  - Maintains a watermark table (last_processed_timestamp per source)
  - Filters files to only those newer than the current watermark
  - Updates the watermark on successful pipeline completion
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_EPOCH = "1970-01-01T00:00:00+00:00"


class WatermarkStore:
    """Persists per-source watermarks in a JSON file."""

    def __init__(self, watermark_file: str | Path):
        self.path = Path(watermark_file)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, dict] = self._load()

    def _load(self) -> dict[str, dict]:
        if self.path.exists():
            try:
                with open(self.path) as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Watermark file is corrupt; resetting.")
        return {}

    def _save(self) -> None:
        with open(self.path, "w") as f:
            json.dump(self._data, f, indent=2)

    def get_watermark(self, source_key: str) -> str:
        """Return the last processed timestamp ISO string for source_key."""
        entry = self._data.get(source_key, {})
        return entry.get("last_processed_timestamp", _EPOCH)

    def set_watermark(
        self,
        source_key: str,
        timestamp: str,
        run_id: str = "",
        files_processed: int = 0,
    ) -> None:
        self._data[source_key] = {
            "source_key": source_key,
            "last_processed_timestamp": timestamp,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "last_run_id": run_id,
            "files_processed_in_last_run": files_processed,
        }
        self._save()
        logger.info("Watermark updated: %s -> %s", source_key, timestamp)

    def get_all(self) -> dict[str, dict]:
        return dict(self._data)

    def reset(self, source_key: str) -> None:
        """Reset a source's watermark to the epoch (force full reload)."""
        if source_key in self._data:
            del self._data[source_key]
            self._save()
            logger.info("Watermark reset for source: %s", source_key)


class IncrementalLoadManager:
    """
    Manages incremental file processing for the FHIR pipeline.

    Usage:
        manager = IncrementalLoadManager("data/watermarks.json")
        pending = manager.get_pending_files("data/raw", source_key="fhir_bundles")
        # ... process pending files ...
        manager.complete_run("fhir_bundles", pending, run_id="abc123")
    """

    def __init__(self, watermark_file: str | Path):
        self.store = WatermarkStore(watermark_file)

    def get_pending_files(
        self,
        directory: str | Path,
        source_key: str,
        file_pattern: str = "*.json",
    ) -> list[Path]:
        """
        Return files in directory that are newer than the current watermark.

        Uses file modification time (mtime) as the comparison basis.
        For production ADLS, this would use blob last-modified metadata.
        """
        directory = Path(directory)
        if not directory.exists():
            logger.warning("Directory does not exist: %s", directory)
            return []

        watermark_str = self.store.get_watermark(source_key)
        watermark_dt = datetime.fromisoformat(watermark_str)

        all_files = sorted(directory.glob(file_pattern))
        pending = []
        for f in all_files:
            file_mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if file_mtime > watermark_dt:
                pending.append(f)
                logger.debug(
                    "Pending: %s (mtime=%s > watermark=%s)",
                    f.name,
                    file_mtime.isoformat(),
                    watermark_str,
                )

        logger.info(
            "Incremental check [%s]: %d/%d files pending (watermark=%s)",
            source_key,
            len(pending),
            len(all_files),
            watermark_str,
        )
        return pending

    def complete_run(
        self,
        source_key: str,
        processed_files: list[Path],
        run_id: str = "",
    ) -> str:
        """
        Call after successfully processing files to advance the watermark.

        The new watermark is set to the latest mtime among processed files
        (or current time if no files were processed).

        Returns the new watermark timestamp string.
        """
        if not processed_files:
            new_watermark = datetime.now(timezone.utc).isoformat()
        else:
            latest_mtime = max(
                datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                for f in processed_files
            )
            new_watermark = latest_mtime.isoformat()

        self.store.set_watermark(
            source_key=source_key,
            timestamp=new_watermark,
            run_id=run_id,
            files_processed=len(processed_files),
        )
        return new_watermark

    def get_watermark(self, source_key: str) -> str:
        return self.store.get_watermark(source_key)

    def reset_watermark(self, source_key: str) -> None:
        """Force a full reload on next run."""
        self.store.reset(source_key)

    def list_sources(self) -> dict[str, dict]:
        return self.store.get_all()

    def is_full_load_needed(
        self,
        source_key: str,
        directory: str | Path,
        file_pattern: str = "*.json",
    ) -> bool:
        """
        Returns True if there are no processed files yet (first run),
        which indicates a full load should be performed.
        """
        watermark = self.get_watermark(source_key)
        return watermark == _EPOCH
