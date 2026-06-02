# FHIR Patient Pipeline

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-0.9%2B-yellow.svg)
![Pandas](https://img.shields.io/badge/pandas-2.0%2B-green.svg)
![License](https://img.shields.io/badge/license-MIT-lightgrey.svg)
![CI](https://github.com/yourorg/fhir-patient-pipeline/actions/workflows/ci.yml/badge.svg)

An end-to-end FHIR R4 patient data pipeline implementing a lakehouse architecture (Bronze/Silver/Gold) for healthcare analytics. Ingests synthetic FHIR bundles, validates resources against R4 schemas, transforms nested JSON into tabular parquet layers, and loads into DuckDB — simulating an Azure Synapse Analytics environment.

Built as a portfolio project demonstrating senior data engineering patterns in the healthcare domain: incremental loading, data quality scoring, watermark-based CDC, and dimensional modelling aligned to FHIR resource semantics.

---

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                        FHIR Patient Pipeline                        │
 └─────────────────────────────────────────────────────────────────────┘

  Source Data (FHIR R4 Bundles)
  ┌─────────────────────────┐
  │  Patient | Encounter    │
  │  Condition | Observation│
  │  Claim                  │
  └────────────┬────────────┘
               │  JSON bundles (*.json)
               ▼
  ┌─────────────────────────┐
  │   INGESTION LAYER       │
  │  fhir_bundle_reader.py  │  ← Reads bundles, extracts resources by type
  │  adls_simulator.py      │  ← Manages landing/bronze/silver/gold zones
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │   VALIDATION LAYER      │
  │  fhir_validator.py      │  ← Required fields, code sets, date logic
  │  data_quality_checks.py │  ← Completeness / Conformity / Consistency / Uniqueness
  └────────────┬────────────┘
               │  Quality score ≥ 70 required to proceed
               ▼
  ┌─────────────────────────┐
  │  BRONZE LAYER  (Parquet)│
  │  bronze_layer.py        │  ← Raw JSON + metadata cols, dedup by (id, versionId)
  │  data/bronze/           │  ← Partitioned: resource_type/dt=YYYY-MM-DD/
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │  SILVER LAYER  (Parquet)│
  │  silver_layer.py        │  ← Flatten nested JSON → typed tabular columns
  │  data/silver/           │  ← One parquet per resource type
  │                         │    - Patient: name, dob, gender, address, telecom
  │                         │    - Encounter: class, period, LOS hours
  │                         │    - Condition: ICD-10, onset, clinical status
  │                         │    - Observation: LOINC, value, unit, effective date
  │                         │    - Claim: total, CPT code, billing period
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │  GOLD LAYER    (Parquet)│
  │  gold_layer.py          │  ← Business aggregations and KPIs
  │  data/gold/             │
  │                         │    patient_encounter_summary
  │                         │    patient_condition_profile
  │                         │    claims_financial_summary
  │                         │    monthly_utilization_trends
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────────────────────────────────────┐
  │  DUCKDB WAREHOUSE  (simulates Azure Synapse Analytics)  │
  │  duckdb_loader.py                                       │
  │                                                         │
  │  Schemas: bronze | silver | gold                        │
  │  Views:   silver.vw_patient_demographics                │
  │           silver.vw_encounter_detail                    │
  │           silver.vw_condition_detail                    │
  │           silver.vw_observation_detail                  │
  │           silver.vw_claim_detail                        │
  │           gold.vw_patient_360                           │
  │           gold.vw_high_cost_patients                    │
  │           gold.vw_chronic_disease_burden                │
  │           gold.vw_encounter_utilization_summary         │
  └─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11 |
| Data format | Apache Parquet (via PyArrow) |
| Warehouse | DuckDB (Synapse simulation) |
| Validation | Custom FHIR R4 schema engine + jsonschema |
| Data manipulation | Pandas 2.x |
| Config | YAML |
| Testing | pytest + pytest-cov |
| CI/CD | GitHub Actions |

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourorg/fhir-patient-pipeline.git
cd fhir-patient-pipeline

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
pip install -e .

# 4. Run the full pipeline
python -m src.pipeline.orchestrator

# 5. Run tests
pytest tests/ -v --cov=src
```

---

## Data Layers Explained

### Bronze Layer
Raw FHIR resources stored as-is with added pipeline metadata:
- `pipeline_run_id`, `ingestion_timestamp`, `source_file`, `bundle_id`
- Deduplication by `(resource_id, version_id)` — keeps the latest when duplicates exist
- Partitioned by ingestion date: `bronze/patient/dt=2024-01-15/patient.parquet`

### Silver Layer
Flattened, typed, and normalized tables — one per FHIR resource type:
- JSON arrays → scalar columns (first name, family name, address line 1, etc.)
- ISO date normalization and type casting
- Patient references denormalized to `patient_id` FK column
- Computed fields: `length_of_stay_hours`, `is_active` (condition flag)

### Gold Layer
Business-level aggregates ready for BI tools:
- **patient_encounter_summary**: encounter counts, class breakdowns, avg/max LOS
- **patient_condition_profile**: active conditions, chronic condition flags, ICD code lists
- **claims_financial_summary**: total billed, claim counts, avg claim value
- **monthly_utilization_trends**: encounter volume by month, class, and status

---

## Project Structure

```
fhir-patient-pipeline/
├── config/
│   └── pipeline_config.yaml       # All pipeline settings
├── data/
│   ├── raw/                       # FHIR R4 bundle JSON files (3 synthetic patients)
│   └── schemas/                   # FHIR R4 validation schemas
├── src/
│   ├── ingestion/
│   │   ├── fhir_bundle_reader.py  # Bundle parsing and resource extraction
│   │   └── adls_simulator.py      # Local ADLS Gen2 zone management
│   ├── validation/
│   │   ├── fhir_validator.py      # Per-resource FHIR R4 validation rules
│   │   └── data_quality_checks.py # Quality scoring engine (0-100)
│   ├── transformation/
│   │   ├── bronze_layer.py        # Raw → Bronze parquet with metadata
│   │   ├── silver_layer.py        # Bronze → Silver normalized tables
│   │   └── gold_layer.py          # Silver → Gold aggregations
│   ├── warehouse/
│   │   ├── duckdb_loader.py       # DuckDB load and view management
│   │   └── sql/                   # DDL and view SQL files
│   └── pipeline/
│       ├── orchestrator.py        # Main pipeline runner with retry logic
│       └── incremental_load.py    # Watermark-based incremental load
├── tests/                         # pytest test suite
└── notebooks/
    └── pipeline_exploration.ipynb # Pipeline output exploration
```

---

## Pipeline Run Example

```
2024-01-15 10:00:00 | orchestrator | INFO | Pipeline run a3f2c1b8 starting
2024-01-15 10:00:00 | fhir_bundle_reader | INFO | Reading bundle: sample_bundle_001.json
2024-01-15 10:00:00 | fhir_bundle_reader | INFO | Ingested: Patient=1, Encounter=3, Condition=5, Observation=8, Claim=2
2024-01-15 10:00:01 | fhir_validator | INFO | Validation complete: 57 resources checked, 0 errors
2024-01-15 10:00:01 | data_quality | INFO | Quality check [Patient]: score=98.5 passed=True
2024-01-15 10:00:01 | bronze_layer | INFO | Bronze [Patient]: wrote 3 records
2024-01-15 10:00:01 | silver_layer | INFO | Silver [Encounter]: produced 9 rows
2024-01-15 10:00:02 | gold_layer | INFO | Gold [patient_encounter_summary]: 3 rows
2024-01-15 10:00:02 | duckdb_loader | INFO | Loaded silver.patient: 3 rows
2024-01-15 10:00:02 | orchestrator | INFO | Pipeline run a3f2c1b8 complete. Status=SUCCESS Duration=2.1s
```

Sample gold output (`patient_360` view):
```
patient_id   | full_name        | total_encounters | chronic_condition_count | total_billed | risk_tier
-------------|-----------------|------------------|------------------------|--------------|----------
patient-001  | Carlos Martinez  | 3                | 3                      | 4435.00      | HIGH
patient-002  | Wei Lin Chen     | 3                | 4                      | 7045.00      | HIGH
patient-003  | Alicia Johnson   | 3                | 3                      | 4135.00      | MEDIUM
```

---

## Configuration

The pipeline is driven by `config/pipeline_config.yaml`. Key settings:

```yaml
pipeline:
  run_mode: full          # full | incremental

validation:
  strict_mode: false
  min_quality_score: 70   # Reject batches below this score

retry:
  max_attempts: 3
  backoff_base_seconds: 2
  backoff_multiplier: 2

warehouse:
  duckdb:
    memory_limit: "2GB"
    threads: 4
```

---

## Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage report
pytest tests/ -v --cov=src --cov-report=html

# Specific test module
pytest tests/test_fhir_validator.py -v
```

---

## Incremental Load

The pipeline supports watermark-based incremental loads:

```python
from src.pipeline.incremental_load import IncrementalLoadManager

manager = IncrementalLoadManager("data/watermarks.json")
pending = manager.get_pending_files("data/raw", source_key="fhir_bundles")
# ... process only the pending (new) files ...
manager.complete_run("fhir_bundles", processed_files=pending, run_id=run_id)
```

Watermarks are persisted in `data/watermarks.json` and survive process restarts.
