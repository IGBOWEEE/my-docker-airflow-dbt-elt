--description: This model calculates bed occupancy rates for different departments in the hospital.

WITH stg_bed_occupancy AS (
    SELECT * FROM {{ ref('stg_bed_occupancy') }}
)
SELECT
    date,
    branch_name,
    state_name,
    total_beds,
    occupied_beds,
    occupancy_rate,
    _loaded_at
FROM stg_bed_occupancy
WHERE date IS NOT NULL
    AND total_beds >= 0
    AND occupied_beds >= 0
    AND occupied_beds <= total_beds
