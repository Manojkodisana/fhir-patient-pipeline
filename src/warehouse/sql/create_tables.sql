-- FHIR Patient Pipeline - DuckDB Table DDL
-- These are created/replaced dynamically by the loader using Parquet scanning.
-- This file is reserved for any static lookup tables or audit infrastructure.

CREATE TABLE IF NOT EXISTS gold.pipeline_run_log (
    run_id          VARCHAR,
    start_time      VARCHAR,
    end_time        VARCHAR,
    status          VARCHAR,
    total_records   BIGINT,
    error_message   VARCHAR,
    created_at      VARCHAR
);

CREATE TABLE IF NOT EXISTS gold.data_quality_scores (
    run_id          VARCHAR,
    resource_type   VARCHAR,
    record_count    BIGINT,
    quality_score   DOUBLE,
    completeness    DOUBLE,
    conformity      DOUBLE,
    consistency     DOUBLE,
    uniqueness      DOUBLE,
    passed_threshold BOOLEAN,
    evaluated_at    VARCHAR
)
