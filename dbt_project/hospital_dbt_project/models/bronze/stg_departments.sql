WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'departments')
    
}}
)
SELECT
    "c1" AS department_id,
	"c2" AS department_name,
	"c3" AS branch_id,
	"c4" AS branch_name,
	"c5" AS state_name,
    CURRENT_TIMESTAMP AS _loaded_at 
    
FROM source