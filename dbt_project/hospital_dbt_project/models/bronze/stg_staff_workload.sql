WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'staff_workload')
    
}}
)

SELECT
    "c1" AS date,
	"c2" AS department_id,
	"c3" AS department_name,
	"c4" AS branch_name,
	"c5" AS state_name,
	"c6" AS number_of_staff,
	"c7" AS patients_served,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source