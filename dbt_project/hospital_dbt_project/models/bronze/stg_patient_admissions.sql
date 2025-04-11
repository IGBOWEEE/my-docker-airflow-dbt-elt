WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'patient_admissions')
    
}}
)

SELECT
    "c1" AS admissions_id,
	"c2" AS patient_id,
	"c3" AS admission_date,
	"c4" AS discharge_date,
	"c5" AS is_readmission,
	"c6" AS branch_name,
	"c7" AS state_name,
	"c8" AS bed_number,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source