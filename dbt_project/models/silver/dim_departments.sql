WITH stg_departments AS (
    SELECT *
    FROM {{ ref('stg_departments') }}
)
SELECT
    department_id,
    department_name,
    branch_id,
    branch_name,
    state_name,
    _loaded_at
FROM stg_departments
WHERE department_id IS NOT NULL
