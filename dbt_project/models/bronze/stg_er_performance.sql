WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'er_performance')
    
}}
)

SELECT  
    "ER Performance ID" AS er_performance_id,
	"Date" AS date,
	"ER Wait Time (minutes)" AS er_wait_time,
	"Patient Inflow" AS patient_inflow,
	"Patient Outflow" AS patient_outflow,
	"Treatment Success Rate (%)" AS treatment_success_rate_percentage,
	"Branch Name" AS branch_name,
	"State" AS state_name,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source