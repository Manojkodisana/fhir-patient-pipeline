-- Gold layer analytical views
-- Business-intelligence-facing views that join and enrich gold aggregated tables.

-- -------------------------------------------------------------------------
-- vw_patient_360
-- Full patient view joining all gold dimensions
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_patient_360 AS
SELECT
    pes.patient_id,
    pes.full_name,
    pes.gender,
    pes.birth_date,
    -- Encounter summary
    pes.total_encounters,
    pes.amb_encounters,
    pes.imp_encounters,
    pes.emer_encounters,
    pes.avg_los_hours,
    pes.max_los_hours,
    pes.latest_encounter_date,
    pes.earliest_encounter_date,
    -- Condition profile
    pcp.total_conditions,
    pcp.active_condition_count,
    pcp.chronic_condition_count,
    pcp.has_chronic_condition,
    pcp.active_icd_codes,
    pcp.active_condition_names,
    -- Financial summary
    cfs.total_claims,
    cfs.total_billed,
    cfs.avg_claim_value,
    cfs.professional_claims,
    cfs.institutional_claims,
    cfs.latest_claim_date,
    -- Derived risk tier based on chronic conditions + encounter volume
    CASE
        WHEN pcp.chronic_condition_count >= 3 AND pes.emer_encounters >= 2 THEN 'HIGH'
        WHEN pcp.chronic_condition_count >= 2 OR pes.imp_encounters >= 2   THEN 'MEDIUM'
        WHEN pcp.has_chronic_condition                                       THEN 'LOW_CHRONIC'
        ELSE 'LOW'
    END                                                       AS risk_tier,
    -- Cost per encounter (where encounters > 0)
    CASE
        WHEN pes.total_encounters > 0
        THEN ROUND(cfs.total_billed / pes.total_encounters, 2)
        ELSE NULL
    END                                                       AS cost_per_encounter
FROM gold.patient_encounter_summary  pes
LEFT JOIN gold.patient_condition_profile pcp
       ON pes.patient_id = pcp.patient_id
LEFT JOIN gold.claims_financial_summary  cfs
       ON pes.patient_id = cfs.patient_id;


-- -------------------------------------------------------------------------
-- vw_high_cost_patients
-- Patients in the top quartile by total billed amount
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_high_cost_patients AS
WITH cost_ranked AS (
    SELECT
        cfs.patient_id,
        cfs.full_name,
        cfs.gender,
        cfs.total_claims,
        cfs.total_billed,
        cfs.avg_claim_value,
        cfs.professional_claims,
        cfs.institutional_claims,
        pcp.active_condition_count,
        pcp.chronic_condition_count,
        pcp.active_icd_codes,
        PERCENT_RANK() OVER (ORDER BY cfs.total_billed)       AS cost_percentile
    FROM gold.claims_financial_summary cfs
    LEFT JOIN gold.patient_condition_profile pcp
           ON cfs.patient_id = pcp.patient_id
)
SELECT
    *,
    CASE
        WHEN cost_percentile >= 0.75 THEN 'Top 25%'
        WHEN cost_percentile >= 0.50 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END                                                       AS cost_tier
FROM cost_ranked
ORDER BY total_billed DESC;


-- -------------------------------------------------------------------------
-- vw_chronic_disease_burden
-- Aggregate view of chronic disease prevalence across the patient population
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_chronic_disease_burden AS
WITH icd_exploded AS (
    -- Unnest pipe-delimited ICD code list
    SELECT
        patient_id,
        TRIM(icd_code.code) AS icd_code
    FROM gold.patient_condition_profile,
         UNNEST(string_split(active_icd_codes, '|')) AS icd_code(code)
    WHERE active_icd_codes IS NOT NULL AND active_icd_codes <> ''
),
chronic_flags AS (
    SELECT
        icd_code,
        COUNT(DISTINCT patient_id) AS patient_count,
        -- Map code prefix to condition group
        CASE
            WHEN icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' OR icd_code LIKE 'E13%' THEN 'Diabetes'
            WHEN icd_code LIKE 'I10%' OR icd_code LIKE 'I11%'                          THEN 'Hypertension'
            WHEN icd_code LIKE 'I50%'                                                   THEN 'Heart Failure'
            WHEN icd_code LIKE 'I48%'                                                   THEN 'Atrial Fibrillation'
            WHEN icd_code LIKE 'N18%'                                                   THEN 'Chronic Kidney Disease'
            WHEN icd_code LIKE 'M06%' OR icd_code LIKE 'M05%'                          THEN 'Rheumatoid Arthritis'
            WHEN icd_code LIKE 'M81%'                                                   THEN 'Osteoporosis'
            WHEN icd_code LIKE 'F32%' OR icd_code LIKE 'F33%'                          THEN 'Depression'
            WHEN icd_code LIKE 'E78%'                                                   THEN 'Hyperlipidemia'
            WHEN icd_code LIKE 'I69%'                                                   THEN 'Post-Stroke Sequelae'
            WHEN icd_code LIKE 'K21%'                                                   THEN 'GERD'
            ELSE 'Other'
        END AS condition_group
    FROM icd_exploded
    GROUP BY icd_code
)
SELECT
    condition_group,
    COUNT(*)                                                  AS distinct_icd_codes,
    SUM(patient_count)                                        AS total_patient_occurrences,
    ROUND(
        100.0 * SUM(patient_count) / NULLIF(
            (SELECT COUNT(DISTINCT patient_id) FROM gold.patient_condition_profile), 0
        ), 1
    )                                                         AS prevalence_pct
FROM chronic_flags
GROUP BY condition_group
ORDER BY total_patient_occurrences DESC;


-- -------------------------------------------------------------------------
-- vw_encounter_utilization_summary
-- Monthly utilization view with rolling trends
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_encounter_utilization_summary AS
SELECT
    year_month,
    encounter_class,
    CASE encounter_class
        WHEN 'AMB'  THEN 'Ambulatory'
        WHEN 'IMP'  THEN 'Inpatient'
        WHEN 'EMER' THEN 'Emergency'
        ELSE encounter_class
    END                                                       AS encounter_class_label,
    status,
    encounter_count,
    avg_los_hours,
    total_los_hours,
    -- Running total per encounter class
    SUM(encounter_count) OVER (
        PARTITION BY encounter_class
        ORDER BY year_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                         AS running_total_by_class,
    -- % of total encounters for the month
    ROUND(
        100.0 * encounter_count / NULLIF(
            SUM(encounter_count) OVER (PARTITION BY year_month), 0
        ), 1
    )                                                         AS pct_of_monthly_volume
FROM gold.monthly_utilization_trends
ORDER BY year_month, encounter_class
