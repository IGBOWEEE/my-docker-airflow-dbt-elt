WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'er_performance')
    
}}
)

SELECT  
    "c1" AS er_performance_id
	"c2" AS date,
	"c3" AS er_wait_time,
	"c4" AS patient_inflow,
	"c5" AS patient_outflow,
	"c6" AS treatment_success_rate_percentage,
	"c7" AS branch_name,
	"c8" AS state_name,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source