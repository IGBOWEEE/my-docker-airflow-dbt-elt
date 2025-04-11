--description: this is a fact table to show the staff workload in the hospital

WITH stg_staff_workload AS (
    SELECT * FROM {{ ref('stg_staff_workload') }}
),
transformed AS (
    SELECT
        date AS workload_date,
        department_id,
        department_name,
        branch_name,
        state_name,
        number_of_staff,
        patients_served,
        ROUND(DIV0NULL(
                patients_served,
                number_of_staff
            ),0) AS patients_per_staff_ratio,
        _loaded_at
    FROM stg_staff_workload
    WHERE date IS NOT NULL
        AND department_id IS NOT NULL
        AND department_name IS NOT NULL
        AND branch_name IS NOT NULL
        AND state_name IS NOT NULL
        AND number_of_staff >= 0
        AND patients_served >= 0
)
SELECT * FROM transformed

