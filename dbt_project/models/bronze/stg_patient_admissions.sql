WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'patient_admissions')
    
}}
)

SELECT
    "Admission ID" AS admissions_id,
	"Patient ID" AS patient_id,
	"Admission Date" AS admission_date,
	"Discharge Date" AS discharge_date,
	"Is Readmission" AS is_readmission,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Bed Number" AS bed_number,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source