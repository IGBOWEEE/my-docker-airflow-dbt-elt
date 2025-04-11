WITH stg_equipment_availability AS (
    SELECT *
    FROM {{ ref('stg_equipment_availability') }}
)
SELECT
    equipment_id,
    equipment_name,
    operational_status,
    branch_name,
    department_name,
    last_maintenance_date,
    _loaded_at
FROM stg_equipment_availability
WHERE equipment_id IS NOT NULL