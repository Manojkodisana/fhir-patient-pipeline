"""
Tests for src/transformation/silver_layer.py

Covers:
  - Patient flattening: name, gender, address, telecom extraction
  - Encounter flattening: class, period, LOS calculation
  - Condition flattening: clinical status, ICD code, dates
  - Observation flattening: LOINC code, value, unit
  - Claim flattening: total amount, billing period, claim type
  - Date parsing and normalization
  - Write and read parquet round-trip
"""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.transformation.silver_layer import SilverLayerProcessor, _safe_parse_date


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bronze_row(raw_resource: dict, **meta) -> dict:
    """Simulate a bronze DataFrame row."""
    base_meta = {
        "resource_id": raw_resource.get("id", ""),
        "source_file": "test_bundle.json",
        "bundle_id": "bundle-test",
        "ingestion_timestamp": "2024-01-15T08:00:00+00:00",
        "pipeline_run_id": "test-run-001",
        "ingestion_date": "2024-01-15",
    }
    base_meta.update(meta)
    base_meta["raw_json"] = json.dumps(raw_resource)
    return base_meta


def make_bronze_df(resources: list[dict]) -> pd.DataFrame:
    rows = [bronze_row(r) for r in resources]
    return pd.DataFrame(rows)


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver"


@pytest.fixture
def processor(tmp_silver):
    return SilverLayerProcessor(silver_zone_path=tmp_silver)


# ---------------------------------------------------------------------------
# Patient flattening
# ---------------------------------------------------------------------------

SAMPLE_PATIENT = {
    "resourceType": "Patient",
    "id": "patient-001",
    "name": [
        {"use": "official", "family": "Martinez", "given": ["Carlos", "Eduardo"]}
    ],
    "gender": "male",
    "birthDate": "1978-04-22",
    "telecom": [
        {"system": "phone", "value": "555-867-5309", "use": "home"},
        {"system": "email", "value": "carlos@email.com", "use": "home"},
    ],
    "address": [
        {
            "use": "home",
            "line": ["1423 Elmwood Drive"],
            "city": "Springfield",
            "state": "IL",
            "postalCode": "62701",
            "country": "US",
        }
    ],
    "identifier": [
        {"use": "official", "system": "http://hospital.example.org/mrn", "value": "MRN-100001"}
    ],
}


def test_patient_flattening_basic_fields(processor):
    bronze_df = make_bronze_df([SAMPLE_PATIENT])
    silver_df = processor.process_resource_type("Patient", bronze_df)

    assert len(silver_df) == 1
    row = silver_df.iloc[0]
    assert row["patient_id"] == "patient-001"
    assert row["family_name"] == "Martinez"
    assert row["first_name"] == "Carlos"
    assert row["middle_name"] == "Eduardo"
    assert row["gender"] == "male"
    assert row["birth_date"] == "1978-04-22"


def test_patient_flattening_address(processor):
    bronze_df = make_bronze_df([SAMPLE_PATIENT])
    silver_df = processor.process_resource_type("Patient", bronze_df)
    row = silver_df.iloc[0]

    assert row["city"] == "Springfield"
    assert row["state"] == "IL"
    assert row["postal_code"] == "62701"
    assert row["country"] == "US"


def test_patient_flattening_telecom(processor):
    bronze_df = make_bronze_df([SAMPLE_PATIENT])
    silver_df = processor.process_resource_type("Patient", bronze_df)
    row = silver_df.iloc[0]

    assert row["phone"] == "555-867-5309"
    assert row["email"] == "carlos@email.com"


def test_patient_flattening_full_name(processor):
    bronze_df = make_bronze_df([SAMPLE_PATIENT])
    silver_df = processor.process_resource_type("Patient", bronze_df)
    row = silver_df.iloc[0]
    assert "Carlos" in row["full_name"]
    assert "Martinez" in row["full_name"]


# ---------------------------------------------------------------------------
# Encounter flattening
# ---------------------------------------------------------------------------

SAMPLE_ENCOUNTER = {
    "resourceType": "Encounter",
    "id": "encounter-001",
    "status": "finished",
    "class": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": "IMP",
        "display": "inpatient encounter",
    },
    "type": [
        {
            "coding": [{"system": "http://snomed.info/sct", "code": "32485007", "display": "Hospital admission"}],
            "text": "Hospital Admission",
        }
    ],
    "subject": {"reference": "Patient/patient-001", "display": "Carlos Martinez"},
    "period": {
        "start": "2023-07-18T14:00:00Z",
        "end": "2023-07-22T11:00:00Z",
    },
}


def test_encounter_flattening_basic(processor):
    bronze_df = make_bronze_df([SAMPLE_ENCOUNTER])
    silver_df = processor.process_resource_type("Encounter", bronze_df)

    assert len(silver_df) == 1
    row = silver_df.iloc[0]
    assert row["encounter_id"] == "encounter-001"
    assert row["patient_id"] == "patient-001"
    assert row["status"] == "finished"
    assert row["encounter_class"] == "IMP"


def test_encounter_los_calculation(processor):
    bronze_df = make_bronze_df([SAMPLE_ENCOUNTER])
    silver_df = processor.process_resource_type("Encounter", bronze_df)
    row = silver_df.iloc[0]

    # 18 July 14:00 to 22 July 11:00 = 93 hours
    assert row["length_of_stay_hours"] is not None
    assert abs(row["length_of_stay_hours"] - 93.0) < 1.0


def test_encounter_los_is_none_for_missing_period(processor):
    enc = {
        "resourceType": "Encounter",
        "id": "enc-no-period",
        "status": "planned",
        "subject": {"reference": "Patient/p-001"},
    }
    bronze_df = make_bronze_df([enc])
    silver_df = processor.process_resource_type("Encounter", bronze_df)
    assert silver_df.iloc[0]["length_of_stay_hours"] is None


# ---------------------------------------------------------------------------
# Condition flattening
# ---------------------------------------------------------------------------

SAMPLE_CONDITION = {
    "resourceType": "Condition",
    "id": "condition-001",
    "clinicalStatus": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "active",
                "display": "Active",
            }
        ]
    },
    "code": {
        "coding": [
            {
                "system": "http://hl7.org/fhir/sid/icd-10-cm",
                "code": "E11.9",
                "display": "Type 2 diabetes mellitus without complications",
            }
        ],
        "text": "Type 2 Diabetes Mellitus",
    },
    "subject": {"reference": "Patient/patient-001"},
    "onsetDateTime": "2015-06-01",
    "abatementDateTime": None,
    "recordedDate": "2015-06-15",
}


def test_condition_flattening_basic(processor):
    bronze_df = make_bronze_df([SAMPLE_CONDITION])
    silver_df = processor.process_resource_type("Condition", bronze_df)

    row = silver_df.iloc[0]
    assert row["condition_id"] == "condition-001"
    assert row["patient_id"] == "patient-001"
    assert row["clinical_status"] == "active"
    assert row["condition_code"] == "E11.9"


def test_condition_is_active_flag(processor):
    bronze_df = make_bronze_df([SAMPLE_CONDITION])
    silver_df = processor.process_resource_type("Condition", bronze_df)
    assert silver_df.iloc[0]["is_active"] == True  # noqa: E712


def test_condition_resolved_not_active(processor):
    resolved = {
        **SAMPLE_CONDITION,
        "id": "cond-resolved",
        "clinicalStatus": {
            "coding": [{"code": "resolved"}]
        },
    }
    bronze_df = make_bronze_df([resolved])
    silver_df = processor.process_resource_type("Condition", bronze_df)
    assert silver_df.iloc[0]["is_active"] == False  # noqa: E712


# ---------------------------------------------------------------------------
# Observation flattening
# ---------------------------------------------------------------------------

SAMPLE_OBSERVATION = {
    "resourceType": "Observation",
    "id": "obs-001",
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "laboratory",
                }
            ]
        }
    ],
    "code": {
        "coding": [{"system": "http://loinc.org", "code": "4548-4", "display": "Hemoglobin A1c"}],
        "text": "HbA1c",
    },
    "subject": {"reference": "Patient/patient-001"},
    "encounter": {"reference": "Encounter/encounter-001"},
    "effectiveDateTime": "2023-03-10T09:30:00Z",
    "valueQuantity": {"value": 8.2, "unit": "%", "system": "http://unitsofmeasure.org", "code": "%"},
}


def test_observation_flattening_basic(processor):
    bronze_df = make_bronze_df([SAMPLE_OBSERVATION])
    silver_df = processor.process_resource_type("Observation", bronze_df)

    row = silver_df.iloc[0]
    assert row["observation_id"] == "obs-001"
    assert row["patient_id"] == "patient-001"
    assert row["encounter_id"] == "encounter-001"
    assert row["loinc_code"] == "4548-4"
    assert row["status"] == "final"


def test_observation_value_and_unit(processor):
    bronze_df = make_bronze_df([SAMPLE_OBSERVATION])
    silver_df = processor.process_resource_type("Observation", bronze_df)
    row = silver_df.iloc[0]

    assert row["value"] == pytest.approx(8.2)
    assert row["unit"] == "%"


def test_observation_effective_date_normalized(processor):
    bronze_df = make_bronze_df([SAMPLE_OBSERVATION])
    silver_df = processor.process_resource_type("Observation", bronze_df)
    row = silver_df.iloc[0]

    # Should be normalized to YYYY-MM-DD
    assert row["effective_date"] == "2023-03-10"


# ---------------------------------------------------------------------------
# Claim flattening
# ---------------------------------------------------------------------------

SAMPLE_CLAIM = {
    "resourceType": "Claim",
    "id": "claim-001",
    "status": "active",
    "type": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                "code": "institutional",
                "display": "Institutional",
            }
        ]
    },
    "use": "claim",
    "patient": {"reference": "Patient/patient-001"},
    "billablePeriod": {"start": "2023-07-18", "end": "2023-07-22"},
    "created": "2023-07-25",
    "total": {"value": 4250.0, "currency": "USD"},
    "item": [
        {
            "sequence": 1,
            "productOrService": {
                "coding": [{"system": "http://www.ama-assn.org/go/cpt", "code": "99221"}]
            },
            "net": {"value": 4250.0, "currency": "USD"},
        }
    ],
}


def test_claim_flattening_basic(processor):
    bronze_df = make_bronze_df([SAMPLE_CLAIM])
    silver_df = processor.process_resource_type("Claim", bronze_df)

    row = silver_df.iloc[0]
    assert row["claim_id"] == "claim-001"
    assert row["patient_id"] == "patient-001"
    assert row["status"] == "active"
    assert row["claim_type"] == "institutional"


def test_claim_total_amount(processor):
    bronze_df = make_bronze_df([SAMPLE_CLAIM])
    silver_df = processor.process_resource_type("Claim", bronze_df)
    row = silver_df.iloc[0]

    assert row["total_amount"] == pytest.approx(4250.0)
    assert row["currency"] == "USD"


def test_claim_billable_period_dates(processor):
    bronze_df = make_bronze_df([SAMPLE_CLAIM])
    silver_df = processor.process_resource_type("Claim", bronze_df)
    row = silver_df.iloc[0]

    assert row["billable_period_start"] == "2023-07-18"
    assert row["billable_period_end"] == "2023-07-22"


def test_claim_cpt_code_extracted(processor):
    bronze_df = make_bronze_df([SAMPLE_CLAIM])
    silver_df = processor.process_resource_type("Claim", bronze_df)
    assert silver_df.iloc[0]["primary_cpt_code"] == "99221"


# ---------------------------------------------------------------------------
# Utility function tests
# ---------------------------------------------------------------------------

def test_safe_parse_date_valid():
    assert _safe_parse_date("2023-03-10") == "2023-03-10"
    assert _safe_parse_date("2023-03-10T09:30:00Z") == "2023-03-10"


def test_safe_parse_date_invalid():
    assert _safe_parse_date("not-a-date") is None
    assert _safe_parse_date(None) is None
    assert _safe_parse_date("") is None


def test_write_and_read_silver_round_trip(processor, tmp_silver):
    bronze_df = make_bronze_df([SAMPLE_PATIENT])
    silver_df = processor.process_resource_type("Patient", bronze_df)
    processor.write_silver("Patient", silver_df)

    read_back = processor.read_silver("Patient")
    assert len(read_back) == 1
    assert read_back.iloc[0]["patient_id"] == "patient-001"
