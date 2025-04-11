{{config(materialized='table', schema='gold', alias='fct_daily_admissions_summary')}}

-- desscription: this table calculates the daily admissions summary for each branch and state

WITH admissions_details AS (
    SELECT
        admissions_id,
        patient_id,
        admission_date,
        branch_name,
        state_name,
        is_readmission,
        length_of_stay_days,
    FROM {{ ref('int_patient_admissions') }}
    WHERE admission_date IS NOT NULL

    )
    
    SELECT
        ad.admission_date,
        ad.branch_name,
        ad.state_name,
        COUNT(DISTINCT ad.patient_id) AS total_admissions,
        COUNT(DISTINCT CASE WHEN ad.is_readmission = TRUE THEN ad.patient_id END) AS total_readmissions,
        ZEROIFNULL(SUM(ad.length_of_stay_days)) AS total_length_of_stay_days,
        AVG(ad.length_of_stay_days) AS avg_length_of_stay_days,
        COUNT(DISTINCT ad.patient_id) as unique_patients_admitted
    FROM admissions_details ad
    GROUP BY
        ad.admission_date,
        ad.branch_name,
        ad.state_name
    ORDER BY
        ad.admission_date,
        ad.branch_name,
        ad.state_name
