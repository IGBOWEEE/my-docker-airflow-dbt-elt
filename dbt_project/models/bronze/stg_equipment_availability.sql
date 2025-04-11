WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'equipment_availability')
    
}}
)
SELECT  
	"Equipment ID" AS equipment_id,
	"Equipment Name" AS equipment_name,
	"Status" AS operational_status,
	"Branch Name" AS branch_name,
	"Department Name" AS department_name,
	"Last Maintenance Date" AS last_maintenance_date,
	CURRENT_TIMESTAMP() AS _loaded_at
FROM source