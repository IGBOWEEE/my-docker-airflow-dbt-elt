WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'bed_occupancy')
    
}}
)

SELECT  
   	"c1" AS date,
	"c2" AS branch_name,
	"c3" AS state_name,
	"c4" AS total_beds ,
	"c5" AS occupied_beds,
	"c6" AS occupancy_rate,
	CURRENT_TIMESTAMP() AS _loaded_at

FROM source