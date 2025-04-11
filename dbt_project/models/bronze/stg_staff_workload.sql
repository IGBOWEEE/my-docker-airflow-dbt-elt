WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'staff_workload')
    
}}
)

SELECT
    "Date" AS date,
	"Department ID" AS department_id,
	"Department Name" AS department_name,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Number of Staff" AS number_of_staff,
	"Patients Served" AS patients_served,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source