WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'equipment_availability')
    
}}
)
SELECT  
	"c1" AS equipment_id,
	"c2" AS equipment_name,
	"c3" AS operational_status,
	"c4" AS branch_name,
	"c5" AS department_name,
	"c6" AS last_maintenance_date,
	CURRENT_TIMESTAMP() AS _loaded_at
FROM source