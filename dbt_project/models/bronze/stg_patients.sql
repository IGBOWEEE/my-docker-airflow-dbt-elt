WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'patients')
    
}}
)

SELECT
    "Patient ID" AS patient_id,
	"Name" AS patient_name,
	"Age" AS patient_age,
	"Gender" AS patient_gender,
	"Phone" AS patient_phone_number,
	"Address" AS patient_address,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Insurance Provider" AS insurance_provider,
	"Insurance Status" AS insurance_status,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source