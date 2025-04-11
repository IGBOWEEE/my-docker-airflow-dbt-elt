WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'department_finance')
    
}}
)

SELECT
   	"Department Financial ID" AS department_financial_id,
	"Department ID" AS department_id,
	"Department Name" AS department_name,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Month" AS year_month,
	"Revenue" AS revenue,
	"Expenses" AS expenses,
	"Total Claims Submitted" AS total_claims_submitted,
	"Insurance Claims Approved" AS insurance_claims_approved,
	"Cost Per Patient" AS cost_per_patient,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source