--description: this model joins admissions with patients and calculates length of stay
-- to create a comprehensive view of patient admissions.    

WITH stg_patient_admissions AS (
    SELECT * FROM {{ ref('stg_patient_admissions') }}
),
dim_patients AS (
    SELECT
        patient_id,
        patient_name,
        patient_gender,
        patient_age
    FROM {{ ref('dim_patients') }}
),
joined AS (
    SELECT  
        pat.patient_id,
        pat.patient_name,
        pat.patient_gender,
        pat.patient_age,
        adm.admissions_id,
        adm.admission_date,
        COALESCE(DATEDIFF('day', adm.admission_date, adm.discharge_date), 0) AS length_of_stay_days,
        IFF(LOWER(adm.is_readmission) = 'yes', TRUE, FALSE) AS is_readmission,
        adm.state_name,
        adm.branch_name,
        adm._loaded_at
    FROM stg_patient_admissions adm
    LEFT JOIN dim_patients pat 
        ON adm.patient_id = pat.patient_id
    WHERE adm.admissions_id IS NOT NULL
        AND ADM.admission_date IS NOT NULL

    )

    SELECT * FROM joined



