WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'departments')
    
}}
)
SELECT
    "Department ID" AS department_id,
	"Department Name" AS department_name,
	"Branch ID" AS branch_id,
	"Branch Name" AS branch_name,
	"State" AS state_name,
    CURRENT_TIMESTAMP AS _loaded_at 
    
FROM source