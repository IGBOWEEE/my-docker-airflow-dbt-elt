WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'bed_occupancy')
    
}}
)
SELECT  
   	"Date" AS date,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Total Beds" AS total_beds ,
	"Occupied Beds" AS occupied_beds,
	"Occupancy Rate (%)" AS occupancy_rate,
	CURRENT_TIMESTAMP() AS _loaded_at

FROM source