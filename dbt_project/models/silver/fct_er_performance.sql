--description: This is a fact table that hosts the daily emergency room performance metrics

WITH stg_er_performance AS (
    SELECT * FROM {{ ref('stg_er_performance') }}
)
SELECT
    er_performance_id,
    date,
    branch_name,
    state_name,
    er_wait_time,
    patient_inflow,
    patient_outflow,
    treatment_success_rate_percentage,
    _loaded_at
FROM stg_er_performance
WHERE date IS NOT NULL
    AND er_wait_time >= 0
    AND patient_inflow >= 0
    AND patient_outflow >= 0
    AND treatment_success_rate_percentage BETWEEN 0 AND 100
