"""
Data Quality Engine

Rule-based quality checks for FHIR resource batches. Evaluates completeness,
conformity, consistency, and uniqueness, producing a quality score (0-100) and
a detailed quality report.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Weight of each quality dimension in the final score
DIMENSION_WEIGHTS = {
    "completeness": 0.35,
    "conformity": 0.25,
    "consistency": 0.20,
    "uniqueness": 0.20,
}


@dataclass
class RuleResult:
    rule_name: str
    dimension: str
    passed: int
    failed: int
    total: int
    details: list[str] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 1.0

    def to_dict(self) -> dict:
        return {
            "rule_name": self.rule_name,
            "dimension": self.dimension,
            "passed": self.passed,
            "failed": self.failed,
            "total": self.total,
            "pass_rate": round(self.pass_rate, 4),
            "details": self.details[:10],  # cap detail lines for readability
        }


@dataclass
class QualityReport:
    resource_type: str
    record_count: int
    quality_score: float  # 0-100
    dimension_scores: dict[str, float] = field(default_factory=dict)
    rule_results: list[RuleResult] = field(default_factory=list)
    passed_pipeline_threshold: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "record_count": self.record_count,
            "quality_score": round(self.quality_score, 2),
            "dimension_scores": {k: round(v, 2) for k, v in self.dimension_scores.items()},
            "passed_pipeline_threshold": self.passed_pipeline_threshold,
            "rules": [r.to_dict() for r in self.rule_results],
        }


# ---------------------------------------------------------------------------
# Field completeness rules per resource type
# ---------------------------------------------------------------------------

COMPLETENESS_RULES: dict[str, list[tuple[str, list[str]]]] = {
    "Patient": [
        ("patient_required_fields", ["id", "resourceType"]),
        ("patient_demographic_fields", ["name", "birthDate", "gender"]),
    ],
    "Encounter": [
        ("encounter_required_fields", ["id", "resourceType", "status", "subject"]),
        ("encounter_timing_fields", ["period"]),
    ],
    "Condition": [
        ("condition_required_fields", ["id", "resourceType", "subject"]),
        ("condition_clinical_fields", ["clinicalStatus", "code"]),
    ],
    "Observation": [
        ("obs_required_fields", ["id", "resourceType", "status", "subject"]),
        ("obs_value_fields", ["valueQuantity", "effectiveDateTime"]),
    ],
    "Claim": [
        ("claim_required_fields", ["id", "resourceType", "status", "patient"]),
        ("claim_financial_fields", ["total", "billablePeriod"]),
    ],
}

VALID_ENCOUNTER_STATUSES = {"planned", "arrived", "triaged", "in-progress", "onleave", "finished", "cancelled", "entered-in-error", "unknown"}
VALID_OBS_STATUSES = {"registered", "preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "unknown"}
VALID_CLAIM_STATUSES = {"active", "cancelled", "draft", "entered-in-error"}
VALID_GENDERS = {"male", "female", "other", "unknown"}


class DataQualityEngine:
    """
    Evaluates data quality for a batch of FHIR resources.

    Runs four categories of checks:
    - Completeness: required fields present and non-null
    - Conformity: field values conform to expected code sets
    - Consistency: date logic and cross-field relationships
    - Uniqueness: no duplicate resource IDs within the batch
    """

    def __init__(self, min_quality_score: float = 70.0):
        self.min_quality_score = min_quality_score

    def run_checks(
        self, resources: list[dict[str, Any]], resource_type: str
    ) -> QualityReport:
        """Run all quality rules on a batch and return a QualityReport."""
        if not resources:
            return QualityReport(
                resource_type=resource_type,
                record_count=0,
                quality_score=100.0,
                dimension_scores={d: 100.0 for d in DIMENSION_WEIGHTS},
                passed_pipeline_threshold=True,
            )

        rule_results: list[RuleResult] = []

        # -- Completeness --
        for rule_name, fields_to_check in COMPLETENESS_RULES.get(resource_type, []):
            rule_results.append(self._check_completeness(resources, rule_name, fields_to_check))

        # -- Conformity --
        rule_results.extend(self._check_conformity(resources, resource_type))

        # -- Consistency --
        rule_results.extend(self._check_consistency(resources, resource_type))

        # -- Uniqueness --
        rule_results.append(self._check_uniqueness(resources, resource_type))

        # -- Score aggregation --
        dimension_scores = self._aggregate_dimension_scores(rule_results)
        quality_score = sum(
            dimension_scores.get(dim, 100.0) * weight
            for dim, weight in DIMENSION_WEIGHTS.items()
        )

        report = QualityReport(
            resource_type=resource_type,
            record_count=len(resources),
            quality_score=quality_score,
            dimension_scores=dimension_scores,
            rule_results=rule_results,
            passed_pipeline_threshold=quality_score >= self.min_quality_score,
        )

        logger.info(
            "Quality check [%s]: score=%.1f passed=%s",
            resource_type,
            quality_score,
            report.passed_pipeline_threshold,
        )
        return report

    def run_all_checks(
        self, resources_by_type: dict[str, list[dict[str, Any]]]
    ) -> dict[str, QualityReport]:
        """Run quality checks for all resource types in the batch."""
        return {
            rt: self.run_checks(records, rt)
            for rt, records in resources_by_type.items()
            if records
        }

    # ------------------------------------------------------------------
    # Completeness
    # ------------------------------------------------------------------

    def _check_completeness(
        self,
        resources: list[dict],
        rule_name: str,
        fields: list[str],
    ) -> RuleResult:
        passed = 0
        failed = 0
        details = []
        for res in resources:
            resource_id = res.get("id", "?")
            all_present = True
            for f in fields:
                val = res.get(f)
                if val is None or val == "" or val == []:
                    all_present = False
                    details.append(f"id={resource_id}: field '{f}' missing or empty")
                    break
            if all_present:
                passed += 1
            else:
                failed += 1

        return RuleResult(
            rule_name=rule_name,
            dimension="completeness",
            passed=passed,
            failed=failed,
            total=len(resources),
            details=details,
        )

    # ------------------------------------------------------------------
    # Conformity
    # ------------------------------------------------------------------

    def _check_conformity(
        self, resources: list[dict], resource_type: str
    ) -> list[RuleResult]:
        results = []

        if resource_type == "Patient":
            results.append(self._check_field_in_set(resources, "gender", VALID_GENDERS, "patient_gender_codes"))

        elif resource_type == "Encounter":
            results.append(self._check_field_in_set(resources, "status", VALID_ENCOUNTER_STATUSES, "encounter_status_codes"))
            results.append(self._check_encounter_class_code(resources))

        elif resource_type == "Observation":
            results.append(self._check_field_in_set(resources, "status", VALID_OBS_STATUSES, "obs_status_codes"))
            results.append(self._check_obs_value_numeric(resources))

        elif resource_type == "Claim":
            results.append(self._check_field_in_set(resources, "status", VALID_CLAIM_STATUSES, "claim_status_codes"))

        elif resource_type == "Condition":
            results.append(self._check_condition_clinical_status(resources))

        return results

    def _check_field_in_set(
        self,
        resources: list[dict],
        field_name: str,
        valid_set: set[str],
        rule_name: str,
    ) -> RuleResult:
        passed, failed = 0, 0
        details = []
        for res in resources:
            val = res.get(field_name)
            if val is None:
                passed += 1  # missing => handled by completeness rule
            elif val in valid_set:
                passed += 1
            else:
                failed += 1
                details.append(f"id={res.get('id', '?')}: invalid {field_name}='{val}'")
        return RuleResult(rule_name=rule_name, dimension="conformity", passed=passed, failed=failed, total=len(resources), details=details)

    def _check_encounter_class_code(self, resources: list[dict]) -> RuleResult:
        valid_codes = {"AMB", "EMER", "IMP", "ACUTE", "NONAC", "OBSENC", "PRENC", "SS", "VR"}
        passed, failed = 0, 0
        details = []
        for res in resources:
            cls = res.get("class")
            if not cls:
                passed += 1
                continue
            code = cls.get("code") if isinstance(cls, dict) else None
            if code is None or code in valid_codes:
                passed += 1
            else:
                failed += 1
                details.append(f"id={res.get('id', '?')}: unknown class code='{code}'")
        return RuleResult(rule_name="encounter_class_codes", dimension="conformity", passed=passed, failed=failed, total=len(resources), details=details)

    def _check_obs_value_numeric(self, resources: list[dict]) -> RuleResult:
        passed, failed = 0, 0
        details = []
        for res in resources:
            vq = res.get("valueQuantity")
            if not vq:
                passed += 1
                continue
            val = vq.get("value") if isinstance(vq, dict) else None
            if val is None or isinstance(val, (int, float)):
                passed += 1
            else:
                failed += 1
                details.append(f"id={res.get('id', '?')}: non-numeric valueQuantity.value='{val}'")
        return RuleResult(rule_name="obs_value_numeric", dimension="conformity", passed=passed, failed=failed, total=len(resources), details=details)

    def _check_condition_clinical_status(self, resources: list[dict]) -> RuleResult:
        valid = {"active", "recurrence", "relapse", "inactive", "remission", "resolved"}
        passed, failed = 0, 0
        details = []
        for res in resources:
            cs = res.get("clinicalStatus")
            if not cs:
                passed += 1
                continue
            codings = cs.get("coding", []) if isinstance(cs, dict) else []
            code = codings[0].get("code") if codings else None
            if code is None or code in valid:
                passed += 1
            else:
                failed += 1
                details.append(f"id={res.get('id', '?')}: invalid clinicalStatus.code='{code}'")
        return RuleResult(rule_name="condition_clinical_status_codes", dimension="conformity", passed=passed, failed=failed, total=len(resources), details=details)

    # ------------------------------------------------------------------
    # Consistency
    # ------------------------------------------------------------------

    def _check_consistency(
        self, resources: list[dict], resource_type: str
    ) -> list[RuleResult]:
        results = []

        if resource_type == "Encounter":
            results.append(self._check_encounter_period_logic(resources))

        elif resource_type == "Condition":
            results.append(self._check_condition_date_logic(resources))

        elif resource_type == "Claim":
            results.append(self._check_claim_period_logic(resources))
            results.append(self._check_claim_total_positive(resources))

        return results

    def _check_encounter_period_logic(self, resources: list[dict]) -> RuleResult:
        passed, failed = 0, 0
        details = []
        for res in resources:
            period = res.get("period", {})
            start = period.get("start") if isinstance(period, dict) else None
            end = period.get("end") if isinstance(period, dict) else None
            if start and end and start > end:
                failed += 1
                details.append(f"id={res.get('id', '?')}: start={start} > end={end}")
            else:
                passed += 1
        return RuleResult(rule_name="encounter_period_logic", dimension="consistency", passed=passed, failed=failed, total=len(resources), details=details)

    def _check_condition_date_logic(self, resources: list[dict]) -> RuleResult:
        passed, failed = 0, 0
        details = []
        for res in resources:
            onset = res.get("onsetDateTime", "")
            abate = res.get("abatementDateTime", "")
            if onset and abate:
                onset_d = onset.split("T")[0]
                abate_d = abate.split("T")[0]
                if onset_d > abate_d:
                    failed += 1
                    details.append(f"id={res.get('id', '?')}: onset={onset} after abatement={abate}")
                    continue
            passed += 1
        return RuleResult(rule_name="condition_date_logic", dimension="consistency", passed=passed, failed=failed, total=len(resources), details=details)

    def _check_claim_period_logic(self, resources: list[dict]) -> RuleResult:
        passed, failed = 0, 0
        details = []
        for res in resources:
            bp = res.get("billablePeriod", {})
            start = bp.get("start") if isinstance(bp, dict) else None
            end = bp.get("end") if isinstance(bp, dict) else None
            if start and end and start > end:
                failed += 1
                details.append(f"id={res.get('id', '?')}: billablePeriod start > end")
            else:
                passed += 1
        return RuleResult(rule_name="claim_period_logic", dimension="consistency", passed=passed, failed=failed, total=len(resources), details=details)

    def _check_claim_total_positive(self, resources: list[dict]) -> RuleResult:
        passed, failed = 0, 0
        details = []
        for res in resources:
            total = res.get("total", {})
            val = total.get("value") if isinstance(total, dict) else None
            if val is not None and isinstance(val, (int, float)) and val < 0:
                failed += 1
                details.append(f"id={res.get('id', '?')}: negative claim total={val}")
            else:
                passed += 1
        return RuleResult(rule_name="claim_total_positive", dimension="consistency", passed=passed, failed=failed, total=len(resources), details=details)

    # ------------------------------------------------------------------
    # Uniqueness
    # ------------------------------------------------------------------

    def _check_uniqueness(self, resources: list[dict], resource_type: str) -> RuleResult:
        seen_ids: dict[str, int] = {}
        duplicate_details = []
        for res in resources:
            rid = res.get("id")
            if rid:
                seen_ids[rid] = seen_ids.get(rid, 0) + 1

        duplicates = {k: v for k, v in seen_ids.items() if v > 1}
        for rid, count in duplicates.items():
            duplicate_details.append(f"id={rid} appears {count} times")

        total = len(resources)
        failed = sum(v - 1 for v in duplicates.values())  # extra copies are failures
        passed = total - failed

        return RuleResult(
            rule_name="resource_id_uniqueness",
            dimension="uniqueness",
            passed=passed,
            failed=failed,
            total=total,
            details=duplicate_details,
        )

    # ------------------------------------------------------------------
    # Score aggregation
    # ------------------------------------------------------------------

    def _aggregate_dimension_scores(
        self, rule_results: list[RuleResult]
    ) -> dict[str, float]:
        dimension_pass_rates: dict[str, list[float]] = {d: [] for d in DIMENSION_WEIGHTS}
        for rr in rule_results:
            if rr.dimension in dimension_pass_rates:
                dimension_pass_rates[rr.dimension].append(rr.pass_rate)

        scores = {}
        for dim, rates in dimension_pass_rates.items():
            scores[dim] = (sum(rates) / len(rates) * 100) if rates else 100.0
        return scores
