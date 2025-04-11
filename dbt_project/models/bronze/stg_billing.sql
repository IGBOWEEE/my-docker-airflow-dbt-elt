WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'billing')
    
}}
)

SELECT
    "Billing ID" AS billing_id,
	"Patient ID" AS patient_id,
	"Date" AS billing_date,
	"Total Amount" AS total_billing_amt,
	"Insurance Covered" AS insurance_covered,
	"Out-of-Pocket" AS out_of_pocket,
	"Payment Method" AS payment_method,
	"Branch Name" AS branch_name,
	"State" AS state_name,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source