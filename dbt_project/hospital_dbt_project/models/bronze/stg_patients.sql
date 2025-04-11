WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'patients')
    
}}
)

SELECT
    "c1" AS patient_id,
	"c2" AS patient_name,
	"c3" AS patient_age,
	"c4" AS patient_gender,
	"c5" AS patient_phone_number,
	"c6" AS patient_address,
	"c7" AS branch_name,
	"c8" AS state_name,
	"c9" AS insurance_provider,
	"c10" AS insurance_status,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source