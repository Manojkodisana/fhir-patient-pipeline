-- Silver layer analytical views
-- These views add computed columns and join enrichment on top of the raw silver tables.

CREATE OR REPLACE VIEW silver.vw_patient_demographics AS
SELECT
    patient_id,
    full_name,
    CONCAT(first_name, ' ', family_name)      AS display_name,
    gender,
    birth_date,
    -- Approximate age calculation (year-based)
    (CAST(strftime(CURRENT_DATE, '%Y') AS INT) - CAST(LEFT(birth_date, 4) AS INT)) AS age_years,
    CASE
        WHEN gender = 'male'   THEN 'M'
        WHEN gender = 'female' THEN 'F'
        ELSE 'U'
    END                                        AS gender_code,
    city,
    state,
    postal_code,
    phone,
    email,
    preferred_language,
    marital_status_code,
    source_file,
    pipeline_run_id
FROM silver.patient;


CREATE OR REPLACE VIEW silver.vw_encounter_detail AS
SELECT
    e.encounter_id,
    e.patient_id,
    p.full_name                                AS patient_name,
    p.gender,
    e.status,
    e.encounter_class,
    CASE e.encounter_class
        WHEN 'AMB'  THEN 'Ambulatory'
        WHEN 'IMP'  THEN 'Inpatient'
        WHEN 'EMER' THEN 'Emergency'
        ELSE e.encounter_class
    END                                        AS encounter_class_label,
    e.type_code,
    e.type_display,
    e.period_start,
    e.period_end,
    e.length_of_stay_hours,
    CASE
        WHEN e.length_of_stay_hours IS NULL THEN NULL
        WHEN e.length_of_stay_hours < 4     THEN 'short_stay'
        WHEN e.length_of_stay_hours < 48    THEN 'observation'
        ELSE 'inpatient'
    END                                        AS los_category,
    e.reason_code,
    e.reason_display,
    e.admit_source_code,
    e.discharge_disposition_code,
    e.service_provider,
    e.pipeline_run_id
FROM silver.encounter e
LEFT JOIN silver.patient p ON e.patient_id = p.patient_id;


CREATE OR REPLACE VIEW silver.vw_condition_detail AS
SELECT
    c.condition_id,
    c.patient_id,
    p.full_name                                AS patient_name,
    c.clinical_status,
    c.verification_status,
    c.condition_code,
    c.condition_display,
    c.onset_date,
    c.abatement_date,
    c.recorded_date,
    c.is_active,
    -- Duration in days (for resolved conditions)
    CASE
        WHEN c.abatement_date <> '' AND c.onset_date <> ''
        THEN CAST(
            julianday(c.abatement_date) - julianday(c.onset_date)
            AS INT
        )
        ELSE NULL
    END                                        AS condition_duration_days,
    c.pipeline_run_id
FROM silver.condition c
LEFT JOIN silver.patient p ON c.patient_id = p.patient_id;


CREATE OR REPLACE VIEW silver.vw_observation_detail AS
SELECT
    o.observation_id,
    o.patient_id,
    p.full_name                                AS patient_name,
    o.encounter_id,
    o.status,
    o.category,
    CASE o.category
        WHEN 'vital-signs' THEN 'Vital Signs'
        WHEN 'laboratory'  THEN 'Laboratory'
        ELSE o.category
    END                                        AS category_label,
    o.loinc_code,
    o.observation_name,
    o.value,
    o.unit,
    o.effective_date,
    o.effective_datetime,
    o.interpretation,
    o.reference_range,
    o.pipeline_run_id
FROM silver.observation o
LEFT JOIN silver.patient p ON o.patient_id = p.patient_id;


CREATE OR REPLACE VIEW silver.vw_claim_detail AS
SELECT
    cl.claim_id,
    cl.patient_id,
    p.full_name                                AS patient_name,
    cl.status,
    cl.claim_type,
    cl.claim_type_display,
    cl.use,
    cl.billable_period_start,
    cl.billable_period_end,
    cl.created_date,
    cl.total_amount,
    cl.currency,
    cl.line_item_count,
    cl.line_item_total,
    cl.primary_cpt_code,
    cl.pipeline_run_id
FROM silver.claim cl
LEFT JOIN silver.patient p ON cl.patient_id = p.patient_id
