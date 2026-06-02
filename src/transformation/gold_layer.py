"""
Gold Layer Processor

Produces business-level aggregations from Silver zone data:
  - patient_encounter_summary: encounters per patient with LOS stats
  - patient_condition_profile: active conditions per patient, chronic flags
  - claims_financial_summary: total billed, claims per patient, avg claim value
  - monthly_utilization_trends: encounter volumes by month and class

Reads from Silver zone parquet files and writes aggregated parquet to Gold zone.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ICD-10 code prefixes that indicate a chronic condition (simplified ruleset)
CHRONIC_CONDITION_PREFIXES = (
    "E10", "E11", "E13",  # Diabetes
    "I10", "I11", "I12", "I13",  # Hypertension
    "I25",  # Chronic ischemic heart disease
    "I50",  # Heart failure
    "I48",  # Atrial fibrillation
    "N18",  # Chronic kidney disease
    "J44",  # COPD
    "J45",  # Asthma
    "M06", "M05",  # Rheumatoid arthritis
    "M81",  # Osteoporosis
    "F32", "F33",  # Depression
    "E78",  # Hyperlipidemia
    "I69",  # Sequelae cerebrovascular
)


def _is_chronic(icd10_code: str) -> bool:
    """Heuristic: check if an ICD-10 code matches known chronic condition prefixes."""
    if not icd10_code:
        return False
    return any(icd10_code.upper().startswith(p) for p in CHRONIC_CONDITION_PREFIXES)


class GoldLayerProcessor:
    """
    Produces analytics-ready aggregated tables in the Gold zone.
    Each method reads from silver and writes a gold parquet.
    """

    def __init__(self, gold_zone_path: str | Path):
        self.gold_zone = Path(gold_zone_path)

    def build_all(
        self,
        patients_df: pd.DataFrame,
        encounters_df: pd.DataFrame,
        conditions_df: pd.DataFrame,
        observations_df: pd.DataFrame,
        claims_df: pd.DataFrame,
    ) -> dict[str, str]:
        """
        Build all gold tables and return a dict of {table_name: output_path}.
        """
        output_paths = {}

        df = self.patient_encounter_summary(patients_df, encounters_df)
        output_paths["patient_encounter_summary"] = str(self._write(df, "patient_encounter_summary"))

        df = self.patient_condition_profile(patients_df, conditions_df)
        output_paths["patient_condition_profile"] = str(self._write(df, "patient_condition_profile"))

        df = self.claims_financial_summary(patients_df, claims_df)
        output_paths["claims_financial_summary"] = str(self._write(df, "claims_financial_summary"))

        df = self.monthly_utilization_trends(encounters_df)
        output_paths["monthly_utilization_trends"] = str(self._write(df, "monthly_utilization_trends"))

        logger.info("Gold layer complete: %d tables written", len(output_paths))
        return output_paths

    # ------------------------------------------------------------------
    # Gold table builders
    # ------------------------------------------------------------------

    def patient_encounter_summary(
        self,
        patients_df: pd.DataFrame,
        encounters_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Per-patient encounter summary:
          total_encounters, by encounter class breakdown,
          avg/min/max length of stay (hours), latest encounter date.
        """
        if encounters_df.empty:
            return pd.DataFrame()

        enc = encounters_df.copy()
        enc["los_hours"] = pd.to_numeric(enc.get("length_of_stay_hours"), errors="coerce")

        agg = (
            enc.groupby("patient_id")
            .agg(
                total_encounters=("encounter_id", "count"),
                amb_encounters=("encounter_class", lambda x: (x == "AMB").sum()),
                imp_encounters=("encounter_class", lambda x: (x == "IMP").sum()),
                emer_encounters=("encounter_class", lambda x: (x == "EMER").sum()),
                avg_los_hours=("los_hours", "mean"),
                max_los_hours=("los_hours", "max"),
                min_los_hours=("los_hours", "min"),
                latest_encounter_date=("period_start", "max"),
                earliest_encounter_date=("period_start", "min"),
            )
            .reset_index()
        )

        agg["avg_los_hours"] = agg["avg_los_hours"].round(2)
        agg["max_los_hours"] = agg["max_los_hours"].round(2)
        agg["min_los_hours"] = agg["min_los_hours"].round(2)

        # Merge patient demographics if available
        if not patients_df.empty and "patient_id" in patients_df.columns:
            pat = patients_df[["patient_id", "full_name", "gender", "birth_date"]].drop_duplicates("patient_id")
            agg = agg.merge(pat, on="patient_id", how="left")

        agg["gold_created_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("Gold [patient_encounter_summary]: %d rows", len(agg))
        return agg

    def patient_condition_profile(
        self,
        patients_df: pd.DataFrame,
        conditions_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Per-patient condition profile:
          active condition count, chronic condition count, list of active ICD codes,
          chronic condition flag (has at least one chronic condition).
        """
        if conditions_df.empty:
            return pd.DataFrame()

        cond = conditions_df.copy()
        cond["is_active_flag"] = cond["clinical_status"].isin(["active", "recurrence", "relapse"])
        cond["is_chronic_flag"] = cond["condition_code"].apply(_is_chronic)

        profile = (
            cond.groupby("patient_id")
            .apply(
                lambda g: pd.Series(
                    {
                        "total_conditions": len(g),
                        "active_condition_count": g["is_active_flag"].sum(),
                        "chronic_condition_count": (g["is_active_flag"] & g["is_chronic_flag"]).sum(),
                        "has_chronic_condition": (g["is_active_flag"] & g["is_chronic_flag"]).any(),
                        "active_icd_codes": "|".join(
                            g[g["is_active_flag"]]["condition_code"].dropna().unique().tolist()
                        ),
                        "active_condition_names": "|".join(
                            g[g["is_active_flag"]]["condition_display"].dropna().unique().tolist()
                        ),
                        "earliest_condition_onset": g["onset_date"].replace("", pd.NA).dropna().min(),
                        "most_recent_condition_onset": g["onset_date"].replace("", pd.NA).dropna().max(),
                    }
                ),
                include_groups=False,
            )
            .reset_index()
        )

        if not patients_df.empty and "patient_id" in patients_df.columns:
            pat = patients_df[["patient_id", "full_name", "gender", "birth_date"]].drop_duplicates("patient_id")
            profile = profile.merge(pat, on="patient_id", how="left")

        profile["gold_created_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("Gold [patient_condition_profile]: %d rows", len(profile))
        return profile

    def claims_financial_summary(
        self,
        patients_df: pd.DataFrame,
        claims_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Per-patient financial summary:
          total billed, claim count, avg claim value, by claim type breakdown,
          most recent claim date.
        """
        if claims_df.empty:
            return pd.DataFrame()

        claims = claims_df.copy()
        claims["total_amount"] = pd.to_numeric(claims["total_amount"], errors="coerce")

        summary = (
            claims.groupby("patient_id")
            .agg(
                total_claims=("claim_id", "count"),
                total_billed=("total_amount", "sum"),
                avg_claim_value=("total_amount", "mean"),
                max_claim_value=("total_amount", "max"),
                min_claim_value=("total_amount", "min"),
                professional_claims=("claim_type", lambda x: (x == "professional").sum()),
                institutional_claims=("claim_type", lambda x: (x == "institutional").sum()),
                latest_claim_date=("created_date", "max"),
                earliest_claim_date=("created_date", "min"),
            )
            .reset_index()
        )

        summary["total_billed"] = summary["total_billed"].round(2)
        summary["avg_claim_value"] = summary["avg_claim_value"].round(2)
        summary["max_claim_value"] = summary["max_claim_value"].round(2)
        summary["min_claim_value"] = summary["min_claim_value"].round(2)

        if not patients_df.empty and "patient_id" in patients_df.columns:
            pat = patients_df[["patient_id", "full_name", "gender"]].drop_duplicates("patient_id")
            summary = summary.merge(pat, on="patient_id", how="left")

        summary["gold_created_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("Gold [claims_financial_summary]: %d rows", len(summary))
        return summary

    def monthly_utilization_trends(self, encounters_df: pd.DataFrame) -> pd.DataFrame:
        """
        Monthly aggregate of encounter volumes by class and status.
        Used for operational reporting and capacity planning.
        """
        if encounters_df.empty:
            return pd.DataFrame()

        enc = encounters_df.copy()
        enc["period_start"] = enc["period_start"].replace("", pd.NA)
        enc = enc.dropna(subset=["period_start"])

        enc["year_month"] = enc["period_start"].str[:7]  # 'YYYY-MM'

        trends = (
            enc.groupby(["year_month", "encounter_class", "status"])
            .agg(
                encounter_count=("encounter_id", "count"),
                avg_los_hours=("length_of_stay_hours", lambda x: pd.to_numeric(x, errors="coerce").mean()),
                total_los_hours=("length_of_stay_hours", lambda x: pd.to_numeric(x, errors="coerce").sum()),
            )
            .reset_index()
        )

        trends["avg_los_hours"] = trends["avg_los_hours"].round(2)
        trends["total_los_hours"] = trends["total_los_hours"].round(2)
        trends["gold_created_at"] = datetime.now(timezone.utc).isoformat()

        logger.info("Gold [monthly_utilization_trends]: %d rows", len(trends))
        return trends

    # ------------------------------------------------------------------
    # IO helpers
    # ------------------------------------------------------------------

    def _write(self, df: pd.DataFrame, table_name: str) -> Path:
        if df.empty:
            logger.warning("Gold [%s]: empty DataFrame, skipping write", table_name)
            return self.gold_zone / f"{table_name}.parquet"

        output_path = self.gold_zone / f"{table_name}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert bool columns to Python bool before writing (pyarrow compatibility)
        for col in df.select_dtypes(include="bool").columns:
            df[col] = df[col].astype(bool)

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(output_path), compression="snappy")
        logger.info("Gold: wrote %s (%d rows) -> %s", table_name, len(df), output_path)
        return output_path

    def read_gold(self, table_name: str) -> pd.DataFrame:
        path = self.gold_zone / f"{table_name}.parquet"
        if not path.exists():
            logger.warning("Gold table not found: %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)
