"""
FHIR R4 Validator

Validates FHIR resources against required field rules, data type checks,
and internal reference consistency. Returns structured ValidationResult objects
with configurable severity levels.
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class ValidationSeverity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ValidationIssue:
    severity: ValidationSeverity
    resource_type: str
    resource_id: str
    field: str
    message: str
    rule: str


@dataclass
class ValidationResult:
    resource_type: str
    resource_id: str
    is_valid: bool = True
    issues: list[ValidationIssue] = field(default_factory=list)

    def add_issue(
        self,
        severity: ValidationSeverity,
        field_name: str,
        message: str,
        rule: str = "CUSTOM",
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity=severity,
                resource_type=self.resource_type,
                resource_id=self.resource_id,
                field=field_name,
                message=message,
                rule=rule,
            )
        )
        if severity == ValidationSeverity.ERROR:
            self.is_valid = False

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]

    def summary(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "issues": [
                {
                    "severity": i.severity.value,
                    "field": i.field,
                    "message": i.message,
                    "rule": i.rule,
                }
                for i in self.issues
            ],
        }


# ---------------------------------------------------------------------------
# Field-level rules per resource type
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: dict[str, list[str]] = {
    "Patient": ["id", "resourceType"],
    "Encounter": ["id", "resourceType", "status", "subject"],
    "Condition": ["id", "resourceType", "subject"],
    "Observation": ["id", "resourceType", "status", "subject"],
    "Claim": ["id", "resourceType", "status", "patient"],
}

RECOMMENDED_FIELDS: dict[str, list[str]] = {
    "Patient": ["name", "birthDate", "gender"],
    "Encounter": ["class", "period"],
    "Condition": ["clinicalStatus", "code", "onsetDateTime"],
    "Observation": ["code", "effectiveDateTime", "valueQuantity"],
    "Claim": ["type", "billablePeriod", "total"],
}

VALID_GENDERS = {"male", "female", "other", "unknown"}
VALID_ENCOUNTER_STATUSES = {
    "planned", "arrived", "triaged", "in-progress", "onleave",
    "finished", "cancelled", "entered-in-error", "unknown",
}
VALID_OBSERVATION_STATUSES = {
    "registered", "preliminary", "final", "amended", "corrected",
    "cancelled", "entered-in-error", "unknown",
}
VALID_CLAIM_STATUSES = {"active", "cancelled", "draft", "entered-in-error"}
VALID_CLINICAL_STATUSES = {
    "active", "recurrence", "relapse", "inactive", "remission", "resolved",
}
VALID_ENCOUNTER_CLASSES = {
    "AMB", "EMER", "IMP", "ACUTE", "NONAC", "OBSENC", "PRENC", "SS", "VR",
}

# Loose ISO date / datetime patterns
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$")
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_valid_date_str(value: str) -> bool:
    return bool(_DATE_RE.match(value))


class FHIRValidator:
    """
    Validates FHIR R4 resources.

    Usage:
        validator = FHIRValidator()
        result = validator.validate_resource(resource_dict)
        if not result.is_valid:
            for err in result.errors:
                print(err)
    """

    def __init__(self, strict_mode: bool = False):
        self.strict_mode = strict_mode

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_resource(self, resource: dict[str, Any]) -> ValidationResult:
        """Validate a single FHIR resource dict."""
        resource_type = resource.get("resourceType", "Unknown")
        resource_id = resource.get("id", "MISSING_ID")
        result = ValidationResult(resource_type=resource_type, resource_id=resource_id)

        self._check_required_fields(resource, result)
        self._check_recommended_fields(resource, result)

        dispatch = {
            "Patient": self._validate_patient,
            "Encounter": self._validate_encounter,
            "Condition": self._validate_condition,
            "Observation": self._validate_observation,
            "Claim": self._validate_claim,
        }
        if resource_type in dispatch:
            dispatch[resource_type](resource, result)
        else:
            result.add_issue(
                ValidationSeverity.WARNING,
                "resourceType",
                f"No validation rules defined for resourceType={resource_type}",
                "UNKNOWN_RESOURCE_TYPE",
            )

        return result

    def validate_batch(
        self, resources: list[dict[str, Any]], resource_type: str | None = None
    ) -> list[ValidationResult]:
        """Validate a list of resources, optionally filtering to a specific type."""
        results = []
        for r in resources:
            if resource_type and r.get("resourceType") != resource_type:
                continue
            results.append(self.validate_resource(r))
        return results

    def validate_references(
        self, resources_by_type: dict[str, list[dict]]
    ) -> list[ValidationIssue]:
        """
        Cross-resource reference validation.
        Checks that Patient references in Encounter/Condition/Observation/Claim
        resolve to actual Patient resources in the bundle.
        """
        issues = []
        patient_ids = {r["id"] for r in resources_by_type.get("Patient", []) if "id" in r}

        ref_checks = [
            ("Encounter", "subject"),
            ("Condition", "subject"),
            ("Observation", "subject"),
            ("Claim", "patient"),
        ]

        for res_type, ref_field in ref_checks:
            for resource in resources_by_type.get(res_type, []):
                ref_obj = resource.get(ref_field, {})
                ref_str = ref_obj.get("reference", "") if isinstance(ref_obj, dict) else ""
                if ref_str.startswith("Patient/"):
                    referenced_id = ref_str.split("/", 1)[1]
                    if referenced_id not in patient_ids:
                        issues.append(
                            ValidationIssue(
                                severity=ValidationSeverity.WARNING,
                                resource_type=res_type,
                                resource_id=resource.get("id", "?"),
                                field=ref_field,
                                message=f"Referenced Patient/{referenced_id} not found in bundle",
                                rule="REF_INTEGRITY",
                            )
                        )
        return issues

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_required_fields(self, resource: dict, result: ValidationResult) -> None:
        rt = result.resource_type
        for field_name in REQUIRED_FIELDS.get(rt, []):
            if not resource.get(field_name):
                result.add_issue(
                    ValidationSeverity.ERROR,
                    field_name,
                    f"Required field '{field_name}' is missing or empty",
                    "REQUIRED_FIELD",
                )

    def _check_recommended_fields(self, resource: dict, result: ValidationResult) -> None:
        rt = result.resource_type
        for field_name in RECOMMENDED_FIELDS.get(rt, []):
            if not resource.get(field_name):
                result.add_issue(
                    ValidationSeverity.WARNING,
                    field_name,
                    f"Recommended field '{field_name}' is missing",
                    "RECOMMENDED_FIELD",
                )

    def _validate_patient(self, resource: dict, result: ValidationResult) -> None:
        gender = resource.get("gender")
        if gender and gender not in VALID_GENDERS:
            result.add_issue(
                ValidationSeverity.ERROR,
                "gender",
                f"Invalid gender '{gender}'. Expected one of {VALID_GENDERS}",
                "INVALID_CODE",
            )

        birth_date = resource.get("birthDate")
        if birth_date:
            if not _DATE_ONLY_RE.match(str(birth_date)):
                result.add_issue(
                    ValidationSeverity.ERROR,
                    "birthDate",
                    f"birthDate '{birth_date}' does not match YYYY-MM-DD format",
                    "INVALID_DATE_FORMAT",
                )

        names = resource.get("name", [])
        if not names:
            result.add_issue(
                ValidationSeverity.WARNING,
                "name",
                "Patient has no name entries",
                "MISSING_NAME",
            )
        elif not isinstance(names, list):
            result.add_issue(
                ValidationSeverity.ERROR,
                "name",
                "Patient.name must be an array",
                "WRONG_TYPE",
            )

    def _validate_encounter(self, resource: dict, result: ValidationResult) -> None:
        status = resource.get("status")
        if status and status not in VALID_ENCOUNTER_STATUSES:
            result.add_issue(
                ValidationSeverity.ERROR,
                "status",
                f"Invalid encounter status '{status}'",
                "INVALID_CODE",
            )

        cls = resource.get("class", {})
        if cls and isinstance(cls, dict):
            cls_code = cls.get("code")
            if cls_code and cls_code not in VALID_ENCOUNTER_CLASSES:
                result.add_issue(
                    ValidationSeverity.WARNING,
                    "class.code",
                    f"Encounter class code '{cls_code}' not in standard list {VALID_ENCOUNTER_CLASSES}",
                    "NONSTANDARD_CODE",
                )

        period = resource.get("period", {})
        if period:
            start_str = period.get("start")
            end_str = period.get("end")
            if start_str and end_str:
                # Simple lexicographic comparison works for ISO strings
                if start_str > end_str:
                    result.add_issue(
                        ValidationSeverity.ERROR,
                        "period",
                        f"Encounter period.start ({start_str}) is after period.end ({end_str})",
                        "DATE_LOGIC",
                    )

        subject = resource.get("subject", {})
        if isinstance(subject, dict):
            ref = subject.get("reference", "")
            if ref and not ref.startswith("Patient/"):
                result.add_issue(
                    ValidationSeverity.WARNING,
                    "subject.reference",
                    f"Expected Patient reference, got '{ref}'",
                    "WRONG_REFERENCE_TYPE",
                )

    def _validate_condition(self, resource: dict, result: ValidationResult) -> None:
        clinical_status = resource.get("clinicalStatus", {})
        if clinical_status and isinstance(clinical_status, dict):
            codings = clinical_status.get("coding", [])
            if codings:
                code = codings[0].get("code")
                if code and code not in VALID_CLINICAL_STATUSES:
                    result.add_issue(
                        ValidationSeverity.ERROR,
                        "clinicalStatus.coding.code",
                        f"Invalid clinical status code '{code}'",
                        "INVALID_CODE",
                    )

        onset = resource.get("onsetDateTime")
        abatement = resource.get("abatementDateTime")
        if onset and abatement:
            onset_clean = onset.split("T")[0]
            abate_clean = abatement.split("T")[0]
            if onset_clean > abate_clean:
                result.add_issue(
                    ValidationSeverity.ERROR,
                    "onsetDateTime/abatementDateTime",
                    f"Condition onset ({onset}) is after abatement ({abatement})",
                    "DATE_LOGIC",
                )

    def _validate_observation(self, resource: dict, result: ValidationResult) -> None:
        status = resource.get("status")
        if status and status not in VALID_OBSERVATION_STATUSES:
            result.add_issue(
                ValidationSeverity.ERROR,
                "status",
                f"Invalid observation status '{status}'",
                "INVALID_CODE",
            )

        value_q = resource.get("valueQuantity")
        if value_q and isinstance(value_q, dict):
            val = value_q.get("value")
            if val is not None and not isinstance(val, (int, float)):
                result.add_issue(
                    ValidationSeverity.ERROR,
                    "valueQuantity.value",
                    f"valueQuantity.value must be numeric, got '{type(val).__name__}'",
                    "WRONG_TYPE",
                )

        effective = resource.get("effectiveDateTime")
        if effective and not _is_valid_date_str(str(effective)):
            result.add_issue(
                ValidationSeverity.WARNING,
                "effectiveDateTime",
                f"effectiveDateTime '{effective}' may not be ISO 8601 compliant",
                "INVALID_DATE_FORMAT",
            )

    def _validate_claim(self, resource: dict, result: ValidationResult) -> None:
        status = resource.get("status")
        if status and status not in VALID_CLAIM_STATUSES:
            result.add_issue(
                ValidationSeverity.ERROR,
                "status",
                f"Invalid claim status '{status}'",
                "INVALID_CODE",
            )

        total = resource.get("total", {})
        if total and isinstance(total, dict):
            val = total.get("value")
            if val is not None and val < 0:
                result.add_issue(
                    ValidationSeverity.ERROR,
                    "total.value",
                    f"Claim total value cannot be negative ({val})",
                    "NEGATIVE_AMOUNT",
                )
            currency = total.get("currency")
            if currency and len(currency) != 3:
                result.add_issue(
                    ValidationSeverity.WARNING,
                    "total.currency",
                    f"Currency code '{currency}' does not look like ISO 4217 (expected 3 chars)",
                    "INVALID_CURRENCY",
                )

        billable = resource.get("billablePeriod", {})
        if billable and isinstance(billable, dict):
            start = billable.get("start")
            end = billable.get("end")
            if start and end and start > end:
                result.add_issue(
                    ValidationSeverity.ERROR,
                    "billablePeriod",
                    f"Claim billablePeriod start ({start}) is after end ({end})",
                    "DATE_LOGIC",
                )
