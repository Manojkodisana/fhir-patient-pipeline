"""
DuckDB Loader

Loads parquet files from each lakehouse zone into a DuckDB database,
simulating Azure Synapse Analytics external tables and views.
Runs sanity checks after each load to verify row counts.
"""

import logging
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

# SQL DDL is stored externally for maintainability
_SQL_DIR = Path(__file__).parent / "sql"


class DuckDBLoader:
    """
    Manages DuckDB warehouse operations:
      - Connect to or create the .duckdb file
      - Load parquet files as tables (using DuckDB's native Parquet scanning)
      - Create analytical views on top of the gold tables
      - Run sanity queries post-load
    """

    def __init__(
        self,
        db_path: str | Path,
        memory_limit: str = "2GB",
        threads: int = 4,
    ):
        self.db_path = str(db_path)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(database=self.db_path)
        self.con.execute(f"SET memory_limit='{memory_limit}'")
        self.con.execute(f"SET threads={threads}")
        logger.info("DuckDB connected: %s (memory_limit=%s, threads=%d)", self.db_path, memory_limit, threads)

    def close(self) -> None:
        self.con.close()

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------

    def initialize_schema(self) -> None:
        """Create database schemas and any persistent DDL objects."""
        self.con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        logger.info("DuckDB schemas initialized: bronze, silver, gold")

        # Run DDL from SQL files if they exist
        create_tables_sql = _SQL_DIR / "create_tables.sql"
        if create_tables_sql.exists():
            self._execute_sql_file(create_tables_sql)

    # ------------------------------------------------------------------
    # Table loading
    # ------------------------------------------------------------------

    def load_bronze(self, bronze_zone_path: str | Path) -> dict[str, int]:
        """
        Scan all parquet files in the bronze zone and register them
        as bronze schema tables.
        """
        bronze_path = Path(bronze_zone_path)
        return self._load_zone_tables(bronze_path, schema="bronze")

    def load_silver(self, silver_zone_path: str | Path) -> dict[str, int]:
        """Load silver parquet files as silver schema tables."""
        silver_path = Path(silver_zone_path)
        return self._load_zone_tables(silver_path, schema="silver")

    def load_gold(self, gold_zone_path: str | Path) -> dict[str, int]:
        """Load gold parquet files as gold schema tables."""
        gold_path = Path(gold_zone_path)
        return self._load_zone_tables(gold_path, schema="gold")

    def _load_zone_tables(
        self, zone_path: Path, schema: str
    ) -> dict[str, int]:
        """
        Scan a zone directory for parquet files and create/replace tables.
        Supports partitioned layouts (resource_type/dt=YYYY-MM-DD/file.parquet).
        """
        row_counts = {}
        if not zone_path.exists():
            logger.warning("Zone path does not exist: %s", zone_path)
            return row_counts

        # Collect resource directories (immediate subdirectories)
        resource_dirs = [d for d in zone_path.iterdir() if d.is_dir() and not d.name.startswith("_")]

        # Also check for flat parquet files directly in the zone
        flat_files = list(zone_path.glob("*.parquet"))
        for f in flat_files:
            table_name = f.stem
            self._register_parquet_table(f"'{str(f)}'", schema, table_name)
            count = self._row_count(schema, table_name)
            row_counts[f"{schema}.{table_name}"] = count
            logger.info("Loaded %s.%s: %d rows", schema, table_name, count)

        for resource_dir in sorted(resource_dirs):
            table_name = resource_dir.name.lower().replace("-", "_")
            # Scan all parquet files recursively under this directory
            parquet_files = list(resource_dir.rglob("*.parquet"))
            if not parquet_files:
                logger.debug("No parquet files found in %s, skipping", resource_dir)
                continue

            # Use glob pattern for partitioned data
            glob_pattern = str(resource_dir / "**" / "*.parquet")
            self._register_parquet_table(f"'{glob_pattern}'", schema, table_name)
            count = self._row_count(schema, table_name)
            row_counts[f"{schema}.{table_name}"] = count
            logger.info("Loaded %s.%s: %d rows", schema, table_name, count)

        return row_counts

    def _register_parquet_table(
        self, parquet_glob: str, schema: str, table_name: str
    ) -> None:
        """Create or replace a table by reading from parquet glob."""
        full_name = f"{schema}.{table_name}"
        sql = f"CREATE OR REPLACE TABLE {full_name} AS SELECT * FROM read_parquet({parquet_glob}, hive_partitioning=true, union_by_name=true)"
        try:
            self.con.execute(sql)
        except duckdb.Error as exc:
            logger.error("Failed to register %s: %s", full_name, exc)
            raise

    def _row_count(self, schema: str, table_name: str) -> int:
        result = self.con.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()
        return result[0] if result else 0

    # ------------------------------------------------------------------
    # View creation
    # ------------------------------------------------------------------

    def create_views(self) -> None:
        """Create analytical SQL views from external files."""
        for sql_file in [_SQL_DIR / "silver_views.sql", _SQL_DIR / "gold_views.sql"]:
            if sql_file.exists():
                logger.info("Creating views from %s", sql_file.name)
                self._execute_sql_file(sql_file)
            else:
                logger.warning("SQL file not found: %s", sql_file)

    # ------------------------------------------------------------------
    # Sanity checks
    # ------------------------------------------------------------------

    def run_sanity_checks(self) -> dict[str, Any]:
        """
        Execute a series of validation queries after load.
        Returns a dict of check_name -> result.
        """
        checks = {}

        checks["silver_patients_count"] = self._safe_count("silver.patient")
        checks["silver_encounters_count"] = self._safe_count("silver.encounter")
        checks["silver_conditions_count"] = self._safe_count("silver.condition")
        checks["silver_observations_count"] = self._safe_count("silver.observation")
        checks["silver_claims_count"] = self._safe_count("silver.claim")

        checks["gold_encounter_summary_count"] = self._safe_count("gold.patient_encounter_summary")
        checks["gold_condition_profile_count"] = self._safe_count("gold.patient_condition_profile")
        checks["gold_financial_summary_count"] = self._safe_count("gold.claims_financial_summary")
        checks["gold_utilization_trends_count"] = self._safe_count("gold.monthly_utilization_trends")

        # Cross-layer consistency check
        silver_patient_ids = self._safe_query(
            "SELECT COUNT(DISTINCT patient_id) FROM silver.patient"
        )
        gold_patient_ids = self._safe_query(
            "SELECT COUNT(DISTINCT patient_id) FROM gold.patient_encounter_summary"
        )
        checks["patient_id_consistency"] = {
            "silver_unique_patients": silver_patient_ids,
            "gold_unique_patients": gold_patient_ids,
            "consistent": silver_patient_ids == gold_patient_ids,
        }

        for key, val in checks.items():
            if isinstance(val, int):
                logger.info("Sanity [%s]: %d", key, val)

        return checks

    def query(self, sql: str) -> list[tuple]:
        """Execute an arbitrary SQL query and return results."""
        return self.con.execute(sql).fetchall()

    def query_df(self, sql: str):  # -> pd.DataFrame
        """Execute a SQL query and return results as a pandas DataFrame."""
        return self.con.execute(sql).df()

    def get_table_names(self) -> list[str]:
        result = self.con.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_schema IN ('bronze', 'silver', 'gold') "
            "ORDER BY table_schema, table_name"
        ).fetchall()
        return [r[0] for r in result]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _safe_count(self, table: str) -> int:
        try:
            result = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return result[0] if result else 0
        except duckdb.Error:
            return -1

    def _safe_query(self, sql: str) -> Any:
        try:
            result = self.con.execute(sql).fetchone()
            return result[0] if result else None
        except duckdb.Error:
            return None

    def _execute_sql_file(self, path: Path) -> None:
        """Execute all statements in a .sql file."""
        sql_text = path.read_text(encoding="utf-8")
        # Split on semicolons and execute each statement
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]
        for stmt in statements:
            try:
                self.con.execute(stmt)
            except duckdb.Error as exc:
                logger.error("SQL error executing statement from %s: %s\nSQL: %s", path.name, exc, stmt[:200])
