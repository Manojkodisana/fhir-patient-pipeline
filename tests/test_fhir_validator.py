"""
Tests for src/validation/fhir_validator.py

Covers:
  - Valid resources pass validation
  - Missing required fields trigger ERROR
  - Invalid code values trigger errors
  - Date logic violations trigger errors
  - Cross-resource reference validation
"""

import pytest
from src.validation.fhir_validator import (
    FHIRValidator,
    ValidationResult,
    ValidationSeverity,
)


@pytest.fixture
def validator():
    return FHIRValidator(strict_mode=False)


# ---------------------------------------------------------------------------
# Patient validation
# ---------------------------------------------------------------------------

def test_valid_patient_passes(validator):
    patient = {
        "resourceType": "Patient",
        "id": "patient-001",
        "name": [{"use": "official", "family": "Smith", "given": ["John"]}],
        "gender": "male",
        "birthDate": "1980-05-15",
    }
    result = validator.validate_resource(patient)
    assert result.is_valid
    assert len(result.errors) == 0


def test_patient_missing_id_fails(validator):
    patient = {"resourceType": "Patient", "gender": "male"}
    result = validator.validate_resource(patient)
    assert not result.is_valid
    error_fields = [e.field for e in result.errors]
    assert "id" in error_fields


def test_patient_invalid_gender_fails(validator):
    patient = {
        "resourceType": "Patient",
        "id": "p-bad-gender",
        "gender": "nonbinary",  # not in FHIR R4 value set
    }
    result = validator.validate_resource(patient)
    assert not result.is_valid
    assert any(e.field == "gender" for e in result.errors)


def test_patient_invalid_birth_date_fails(validator):
    patient = {
        "resourceType": "Patient",
        "id": "p-bad-date",
        "birthDate": "15/05/1980",  # wrong format
    }
    result = validator.validate_resource(patient)
    assert not result.is_valid
    assert any(e.field == "birthDate" for e in result.errors)


def test_patient_missing_name_issues_warning(validator):
    patient = {
        "resourceType": "Patient",
        "id": "p-no-name",
        "gender": "female",
        "birthDate": "1990-01-01",
    }
    result = validator.validate_resource(patient)
    # Missing name is a WARNING, not an ERROR
    warning_fields = [w.field for w in result.warnings]
    assert "name" in warning_fields


# ---------------------------------------------------------------------------
# Encounter validation
# ---------------------------------------------------------------------------

def test_valid_encounter_passes(validator):
    encounter = {
        "resourceType": "Encounter",
        "id": "enc-001",
        "status": "finished",
        "subject": {"reference": "Patient/patient-001"},
        "period": {"start": "2023-01-01T09:00:00Z", "end": "2023-01-01T10:00:00Z"},
    }
    result = validator.validate_resource(encounter)
    assert result.is_valid


def test_encounter_missing_status_fails(validator):
    encounter = {
        "resourceType": "Encounter",
        "id": "enc-bad",
        "subject": {"reference": "Patient/patient-001"},
    }
    result = validator.validate_resource(encounter)
    assert not result.is_valid
    assert any(e.field == "status" for e in result.errors)


def test_encounter_invalid_status_fails(validator):
    encounter = {
        "resourceType": "Encounter",
        "id": "enc-bad-status",
        "status": "invalidstatus",
        "subject": {"reference": "Patient/patient-001"},
    }
    result = validator.validate_resource(encounter)
    assert not result.is_valid


def test_encounter_period_start_after_end_fails(validator):
    encounter = {
        "resourceType": "Encounter",
        "id": "enc-bad-period",
        "status": "finished",
        "subject": {"reference": "Patient/patient-001"},
        "period": {
            "start": "2023-03-15T10:00:00Z",
            "end": "2023-03-14T10:00:00Z",  # before start
        },
    }
    result = validator.validate_resource(encounter)
    assert not result.is_valid
    assert any(e.rule == "DATE_LOGIC" for e in result.errors)


# ---------------------------------------------------------------------------
# Condition validation
# ---------------------------------------------------------------------------

def test_valid_condition_passes(validator):
    condition = {
        "resourceType": "Condition",
        "id": "cond-001",
        "subject": {"reference": "Patient/patient-001"},
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
        },
    }
    result = validator.validate_resource(condition)
    assert result.is_valid


def test_condition_invalid_clinical_status_fails(validator):
    condition = {
        "resourceType": "Condition",
        "id": "cond-bad",
        "subject": {"reference": "Patient/patient-001"},
        "clinicalStatus": {
            "coding": [{"code": "definitely-not-valid"}]
        },
    }
    result = validator.validate_resource(condition)
    assert not result.is_valid


def test_condition_onset_after_abatement_fails(validator):
    condition = {
        "resourceType": "Condition",
        "id": "cond-bad-dates",
        "subject": {"reference": "Patient/patient-001"},
        "onsetDateTime": "2023-06-01",
        "abatementDateTime": "2023-05-01",  # abatement before onset
    }
    result = validator.validate_resource(condition)
    assert not result.is_valid
    assert any(e.rule == "DATE_LOGIC" for e in result.errors)


# ---------------------------------------------------------------------------
# Observation validation
# ---------------------------------------------------------------------------

def test_valid_observation_passes(validator):
    obs = {
        "resourceType": "Observation",
        "id": "obs-001",
        "status": "final",
        "subject": {"reference": "Patient/patient-001"},
        "valueQuantity": {"value": 120.0, "unit": "mmHg"},
    }
    result = validator.validate_resource(obs)
    assert result.is_valid


def test_observation_invalid_status_fails(validator):
    obs = {
        "resourceType": "Observation",
        "id": "obs-bad",
        "status": "DONE",  # not a valid FHIR observation status
        "subject": {"reference": "Patient/patient-001"},
    }
    result = validator.validate_resource(obs)
    assert not result.is_valid


# ---------------------------------------------------------------------------
# Claim validation
# ---------------------------------------------------------------------------

def test_valid_claim_passes(validator):
    claim = {
        "resourceType": "Claim",
        "id": "claim-001",
        "status": "active",
        "patient": {"reference": "Patient/patient-001"},
        "total": {"value": 500.0, "currency": "USD"},
    }
    result = validator.validate_resource(claim)
    assert result.is_valid


def test_claim_negative_total_fails(validator):
    claim = {
        "resourceType": "Claim",
        "id": "claim-bad-total",
        "status": "active",
        "patient": {"reference": "Patient/patient-001"},
        "total": {"value": -100.0, "currency": "USD"},
    }
    result = validator.validate_resource(claim)
    assert not result.is_valid
    assert any(e.rule == "NEGATIVE_AMOUNT" for e in result.errors)


def test_claim_invalid_status_fails(validator):
    claim = {
        "resourceType": "Claim",
        "id": "claim-bad-status",
        "status": "submitted",  # not valid
        "patient": {"reference": "Patient/patient-001"},
    }
    result = validator.validate_resource(claim)
    assert not result.is_valid


# ---------------------------------------------------------------------------
# Cross-resource reference validation
# ---------------------------------------------------------------------------

def test_reference_validation_catches_missing_patient():
    validator = FHIRValidator()
    resources_by_type = {
        "Patient": [],  # empty -- no patients
        "Encounter": [
            {
                "resourceType": "Encounter",
                "id": "enc-orphan",
                "status": "finished",
                "subject": {"reference": "Patient/ghost-patient"},
            }
        ],
        "Condition": [],
        "Observation": [],
        "Claim": [],
    }
    issues = validator.validate_references(resources_by_type)
    assert len(issues) >= 1
    assert any("ghost-patient" in i.message for i in issues)


def test_reference_validation_passes_when_patients_present():
    validator = FHIRValidator()
    resources_by_type = {
        "Patient": [{"resourceType": "Patient", "id": "p-001"}],
        "Encounter": [
            {
                "resourceType": "Encounter",
                "id": "enc-001",
                "status": "finished",
                "subject": {"reference": "Patient/p-001"},
            }
        ],
        "Condition": [],
        "Observation": [],
        "Claim": [],
    }
    issues = validator.validate_references(resources_by_type)
    assert len(issues) == 0


# ---------------------------------------------------------------------------
# Batch validation
# ---------------------------------------------------------------------------

def test_batch_validation_returns_result_per_resource():
    validator = FHIRValidator()
    patients = [
        {"resourceType": "Patient", "id": f"p-{i}", "gender": "male", "name": [{"family": "Test"}]}
        for i in range(5)
    ]
    results = validator.validate_batch(patients, resource_type="Patient")
    assert len(results) == 5
    assert all(isinstance(r, ValidationResult) for r in results)


def test_validation_result_summary_structure():
    validator = FHIRValidator()
    patient = {
        "resourceType": "Patient",
        "id": "p-summary-test",
        "gender": "female",
        "birthDate": "1995-08-20",
        "name": [{"family": "Jones", "given": ["Emily"]}],
    }
    result = validator.validate_resource(patient)
    summary = result.summary()
    assert "is_valid" in summary
    assert "error_count" in summary
    assert "warning_count" in summary
    assert "issues" in summary
