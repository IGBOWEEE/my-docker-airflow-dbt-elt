WITH stg_patients AS (
    SELECT *
    FROM {{ ref('stg_patients') }}
),

cleaned AS (
    SELECT
        patient_id,
        patient_name,
        patient_age,
        patient_gender,
        patient_address,
        branch_name,
        state_name,
        insurance_provider,
        TRIM(REGEXP_REPLACE(patient_phone_number, '[^0-9]', '')) AS patient_phone_number,
        _loaded_at,
        insurance_status
    
    FROM stg_patients
)

SELECT
    patient_id,
    patient_name,
    patient_age,
    patient_gender,
    patient_address,
    branch_name,
    state_name,
    patient_phone_number,
    insurance_provider,
    insurance_status,
    _loaded_at
FROM cleaned