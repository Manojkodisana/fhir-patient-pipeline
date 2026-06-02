"""
Silver Layer Processor

Normalizes and flattens FHIR JSON into clean tabular structures:
  - Patient: demographics, contact info, address
  - Encounter: type, class, period, status, subject
  - Condition: code, onset/abatement, clinical status, subject
  - Observation: LOINC code, value/unit, effective date, category
  - Claim: total, billing period, status, claim type

Reads from Bronze zone parquet files and writes normalized parquet to Silver zone.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)


def _safe_parse_date(value: Any, output_format: str = "%Y-%m-%d") -> str | None:
    """Parse an ISO date/datetime string into a standard format. Returns None on failure."""
    if not value or not isinstance(value, str):
        return None
    try:
        return dateutil_parser.parse(value).strftime(output_format)
    except (ValueError, OverflowError):
        return None


def _get_first_coding(coding_list: list[dict], field: str = "code") -> str | None:
    if not coding_list or not isinstance(coding_list, list):
        return None
    first = coding_list[0]
    return first.get(field) if isinstance(first, dict) else None


def _get_codeable_concept(obj: dict | None, prefer_text: bool = False) -> tuple[str | None, str | None]:
    """Extract (code, display) from a CodeableConcept dict."""
    if not obj or not isinstance(obj, dict):
        return None, None
    if prefer_text and obj.get("text"):
        return _get_first_coding(obj.get("coding", []), "code"), obj.get("text")
    codings = obj.get("coding", [])
    code = _get_first_coding(codings, "code")
    display = _get_first_coding(codings, "display") or obj.get("text")
    return code, display


class SilverLayerProcessor:
    """
    Reads bronze parquet files and produces normalized silver tables.
    One output parquet per resource type in the silver zone.
    """

    def __init__(self, silver_zone_path: str | Path):
        self.silver_zone = Path(silver_zone_path)

    def process_resource_type(
        self, resource_type: str, bronze_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Flatten a bronze DataFrame for a specific resource type.
        The bronze DataFrame must have a 'raw_json' column.
        """
        if bronze_df.empty:
            logger.info("Silver [%s]: empty input", resource_type)
            return pd.DataFrame()

        dispatch = {
            "Patient": self._flatten_patients,
            "Encounter": self._flatten_encounters,
            "Condition": self._flatten_conditions,
            "Observation": self._flatten_observations,
            "Claim": self._flatten_claims,
        }
        if resource_type not in dispatch:
            raise ValueError(f"No silver flattening logic for resource type: {resource_type}")

        resources = [json.loads(row) for row in bronze_df["raw_json"]]
        # Attach bronze metadata
        meta_cols = ["resource_id", "source_file", "bundle_id", "ingestion_timestamp", "pipeline_run_id", "ingestion_date"]
        meta = bronze_df[[c for c in meta_cols if c in bronze_df.columns]].to_dict("records")

        silver_df = dispatch[resource_type](resources, meta)
        logger.info("Silver [%s]: produced %d rows", resource_type, len(silver_df))
        return silver_df

    def write_silver(self, resource_type: str, df: pd.DataFrame) -> Path:
        """Write silver DataFrame to parquet."""
        if df.empty:
            logger.warning("Silver [%s]: nothing to write", resource_type)
            return self.silver_zone / resource_type.lower() / f"{resource_type.lower()}.parquet"

        rt_lower = resource_type.lower()
        output_path = self.silver_zone / rt_lower / f"{rt_lower}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(output_path), compression="snappy")
        logger.info("Silver [%s]: wrote to %s", resource_type, output_path)
        return output_path

    def read_silver(self, resource_type: str) -> pd.DataFrame:
        path = self.silver_zone / resource_type.lower() / f"{resource_type.lower()}.parquet"
        if not path.exists():
            logger.warning("Silver file not found: %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # Resource-specific flattening
    # ------------------------------------------------------------------

    def _flatten_patients(
        self, resources: list[dict], meta: list[dict]
    ) -> pd.DataFrame:
        rows = []
        for i, r in enumerate(resources):
            m = meta[i] if i < len(meta) else {}

            # Name
            names = r.get("name", [])
            official_name = next((n for n in names if n.get("use") == "official"), names[0] if names else {})
            given_names = official_name.get("given", [])
            first_name = given_names[0] if given_names else ""
            middle_name = given_names[1] if len(given_names) > 1 else ""
            family_name = official_name.get("family", "")
            full_name = f"{first_name} {family_name}".strip()

            # Address
            addresses = r.get("address", [])
            home_addr = next((a for a in addresses if a.get("use") == "home"), addresses[0] if addresses else {})
            address_lines = home_addr.get("line", [])

            # Telecom
            telecoms = r.get("telecom", [])
            phone = next((t.get("value") for t in telecoms if t.get("system") == "phone"), None)
            email = next((t.get("value") for t in telecoms if t.get("system") == "email"), None)

            # Marital status
            marital = r.get("maritalStatus", {})
            marital_code, _ = _get_codeable_concept(marital)

            # Language
            comms = r.get("communication", [])
            preferred_lang = None
            for comm in comms:
                if comm.get("preferred"):
                    lang = comm.get("language", {})
                    codings = lang.get("coding", [])
                    preferred_lang = _get_first_coding(codings, "code")
                    break

            row = {
                "patient_id": r.get("id", ""),
                "mrn": self._extract_identifier(r.get("identifier", []), system="mrn"),
                "first_name": first_name,
                "middle_name": middle_name,
                "family_name": family_name,
                "full_name": full_name,
                "gender": r.get("gender", ""),
                "birth_date": _safe_parse_date(r.get("birthDate")),
                "active": r.get("active", True),
                "address_line1": address_lines[0] if address_lines else "",
                "address_line2": address_lines[1] if len(address_lines) > 1 else "",
                "city": home_addr.get("city", ""),
                "state": home_addr.get("state", ""),
                "postal_code": home_addr.get("postalCode", ""),
                "country": home_addr.get("country", ""),
                "phone": phone or "",
                "email": email or "",
                "marital_status_code": marital_code or "",
                "preferred_language": preferred_lang or "",
                # pipeline metadata
                "source_file": m.get("source_file", ""),
                "bundle_id": m.get("bundle_id", ""),
                "ingestion_timestamp": m.get("ingestion_timestamp", ""),
                "pipeline_run_id": m.get("pipeline_run_id", ""),
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def _flatten_encounters(
        self, resources: list[dict], meta: list[dict]
    ) -> pd.DataFrame:
        rows = []
        for i, r in enumerate(resources):
            m = meta[i] if i < len(meta) else {}

            cls = r.get("class", {})
            encounter_class = cls.get("code", "") if isinstance(cls, dict) else ""

            types = r.get("type", [])
            type_code, type_display = (None, None)
            if types:
                type_code, type_display = _get_codeable_concept(types[0], prefer_text=True)

            period = r.get("period", {})
            period_start = _safe_parse_date(period.get("start"), "%Y-%m-%dT%H:%M:%S") if period else None
            period_end = _safe_parse_date(period.get("end"), "%Y-%m-%dT%H:%M:%S") if period else None

            # Length of stay in hours
            los_hours = None
            if period_start and period_end:
                try:
                    start_dt = dateutil_parser.parse(period.get("start"))
                    end_dt = dateutil_parser.parse(period.get("end"))
                    los_hours = round((end_dt - start_dt).total_seconds() / 3600, 2)
                except Exception:
                    pass

            subject_ref = (r.get("subject") or {}).get("reference", "")
            patient_id = subject_ref.split("/", 1)[1] if "/" in subject_ref else subject_ref

            reason_code, reason_display = (None, None)
            reason_codes = r.get("reasonCode", [])
            if reason_codes:
                reason_code, reason_display = _get_codeable_concept(reason_codes[0], prefer_text=True)

            hosp = r.get("hospitalization", {}) or {}
            admit_src_code, _ = _get_codeable_concept(hosp.get("admitSource"))
            discharge_code, _ = _get_codeable_concept(hosp.get("dischargeDisposition"))

            row = {
                "encounter_id": r.get("id", ""),
                "patient_id": patient_id,
                "status": r.get("status", ""),
                "encounter_class": encounter_class,
                "type_code": type_code or "",
                "type_display": type_display or "",
                "period_start": period_start or "",
                "period_end": period_end or "",
                "length_of_stay_hours": los_hours,
                "reason_code": reason_code or "",
                "reason_display": reason_display or "",
                "admit_source_code": admit_src_code or "",
                "discharge_disposition_code": discharge_code or "",
                "service_provider": (r.get("serviceProvider") or {}).get("display", ""),
                "source_file": m.get("source_file", ""),
                "bundle_id": m.get("bundle_id", ""),
                "ingestion_timestamp": m.get("ingestion_timestamp", ""),
                "pipeline_run_id": m.get("pipeline_run_id", ""),
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def _flatten_conditions(
        self, resources: list[dict], meta: list[dict]
    ) -> pd.DataFrame:
        rows = []
        for i, r in enumerate(resources):
            m = meta[i] if i < len(meta) else {}

            clinical_status_code = None
            cs = r.get("clinicalStatus", {})
            if cs and isinstance(cs, dict):
                cs_codings = cs.get("coding", [])
                clinical_status_code = _get_first_coding(cs_codings, "code")

            verification_code = None
            vs = r.get("verificationStatus", {})
            if vs and isinstance(vs, dict):
                vs_codings = vs.get("coding", [])
                verification_code = _get_first_coding(vs_codings, "code")

            condition_code, condition_display = _get_codeable_concept(r.get("code"), prefer_text=True)

            category_code = None
            categories = r.get("category", [])
            if categories:
                category_code, _ = _get_codeable_concept(categories[0])

            subject_ref = (r.get("subject") or {}).get("reference", "")
            patient_id = subject_ref.split("/", 1)[1] if "/" in subject_ref else subject_ref

            is_active = clinical_status_code in ("active", "recurrence", "relapse") if clinical_status_code else None
            is_chronic = None  # set in gold layer

            row = {
                "condition_id": r.get("id", ""),
                "patient_id": patient_id,
                "clinical_status": clinical_status_code or "",
                "verification_status": verification_code or "",
                "category": category_code or "",
                "condition_code": condition_code or "",
                "condition_display": (r.get("code") or {}).get("text", condition_display or ""),
                "onset_date": _safe_parse_date(r.get("onsetDateTime")) or "",
                "abatement_date": _safe_parse_date(r.get("abatementDateTime")) or "",
                "recorded_date": _safe_parse_date(r.get("recordedDate")) or "",
                "is_active": is_active,
                "source_file": m.get("source_file", ""),
                "bundle_id": m.get("bundle_id", ""),
                "ingestion_timestamp": m.get("ingestion_timestamp", ""),
                "pipeline_run_id": m.get("pipeline_run_id", ""),
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def _flatten_observations(
        self, resources: list[dict], meta: list[dict]
    ) -> pd.DataFrame:
        rows = []
        for i, r in enumerate(resources):
            m = meta[i] if i < len(meta) else {}

            obs_code, obs_display = _get_codeable_concept(r.get("code"), prefer_text=True)
            obs_text = (r.get("code") or {}).get("text", obs_display or "")

            categories = r.get("category", [])
            category_code = None
            if categories:
                cat_codings = categories[0].get("coding", []) if isinstance(categories[0], dict) else []
                category_code = _get_first_coding(cat_codings, "code")

            value_q = r.get("valueQuantity") or {}
            obs_value = value_q.get("value") if isinstance(value_q, dict) else None
            obs_unit = value_q.get("unit", "") if isinstance(value_q, dict) else ""
            obs_unit_code = value_q.get("code", "") if isinstance(value_q, dict) else ""

            subject_ref = (r.get("subject") or {}).get("reference", "")
            patient_id = subject_ref.split("/", 1)[1] if "/" in subject_ref else subject_ref

            encounter_ref = (r.get("encounter") or {}).get("reference", "")
            encounter_id = encounter_ref.split("/", 1)[1] if "/" in encounter_ref else ""

            interpretation = None
            interp_list = r.get("interpretation", [])
            if interp_list:
                interp_code, _ = _get_codeable_concept(interp_list[0])
                interpretation = interp_code

            ref_range_text = None
            ref_ranges = r.get("referenceRange", [])
            if ref_ranges and isinstance(ref_ranges[0], dict):
                ref_range_text = ref_ranges[0].get("text")

            effective_raw = r.get("effectiveDateTime", "")
            row = {
                "observation_id": r.get("id", ""),
                "patient_id": patient_id,
                "encounter_id": encounter_id,
                "status": r.get("status", ""),
                "category": category_code or "",
                "loinc_code": obs_code or "",
                "observation_name": obs_text,
                "value": obs_value,
                "unit": obs_unit,
                "unit_code": obs_unit_code,
                "effective_date": _safe_parse_date(effective_raw) or "",
                "effective_datetime": _safe_parse_date(effective_raw, "%Y-%m-%dT%H:%M:%S") or "",
                "interpretation": interpretation or "",
                "reference_range": ref_range_text or "",
                "source_file": m.get("source_file", ""),
                "bundle_id": m.get("bundle_id", ""),
                "ingestion_timestamp": m.get("ingestion_timestamp", ""),
                "pipeline_run_id": m.get("pipeline_run_id", ""),
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def _flatten_claims(
        self, resources: list[dict], meta: list[dict]
    ) -> pd.DataFrame:
        rows = []
        for i, r in enumerate(resources):
            m = meta[i] if i < len(meta) else {}

            claim_type_code, claim_type_display = _get_codeable_concept(r.get("type"))

            patient_ref = (r.get("patient") or {}).get("reference", "")
            patient_id = patient_ref.split("/", 1)[1] if "/" in patient_ref else patient_ref

            billable = r.get("billablePeriod") or {}
            billable_start = _safe_parse_date(billable.get("start")) if billable else None
            billable_end = _safe_parse_date(billable.get("end")) if billable else None

            total = r.get("total") or {}
            total_value = total.get("value") if isinstance(total, dict) else None
            total_currency = total.get("currency", "USD") if isinstance(total, dict) else "USD"

            # Count and sum line items
            items = r.get("item", [])
            item_count = len(items)
            line_item_total = sum(
                it.get("net", {}).get("value", 0)
                for it in items
                if isinstance(it.get("net"), dict)
            )

            # Primary procedure/service code
            primary_cpt = None
            if items:
                first_item = items[0]
                pos_code, _ = _get_codeable_concept(first_item.get("productOrService"))
                primary_cpt = pos_code

            row = {
                "claim_id": r.get("id", ""),
                "patient_id": patient_id,
                "status": r.get("status", ""),
                "claim_type": claim_type_code or "",
                "claim_type_display": claim_type_display or "",
                "use": r.get("use", ""),
                "billable_period_start": billable_start or "",
                "billable_period_end": billable_end or "",
                "created_date": _safe_parse_date(r.get("created")) or "",
                "total_amount": total_value,
                "currency": total_currency,
                "line_item_count": item_count,
                "line_item_total": line_item_total,
                "primary_cpt_code": primary_cpt or "",
                "source_file": m.get("source_file", ""),
                "bundle_id": m.get("bundle_id", ""),
                "ingestion_timestamp": m.get("ingestion_timestamp", ""),
                "pipeline_run_id": m.get("pipeline_run_id", ""),
            }
            rows.append(row)

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_identifier(identifiers: list[dict], system: str = "") -> str:
        """Extract identifier value by system keyword match."""
        for ident in identifiers:
            if not isinstance(ident, dict):
                continue
            sys = ident.get("system", "").lower()
            if not system or system.lower() in sys:
                return ident.get("value", "")
        return identifiers[0].get("value", "") if identifiers else ""
