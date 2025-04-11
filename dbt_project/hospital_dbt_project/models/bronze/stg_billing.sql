WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'billing')
    
}}
)

SELECT
    "c1" AS billing_id,
	"c2" AS patient_id,
	"c3" AS billing_date,
	"c4" AS total_billing_amt,
	"c5" AS insurance_covered,
	"c6" AS out_of_pocket,
	"c7" AS payment_method,
	"c8" AS branch_name,
	"c9" AS state_name,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source