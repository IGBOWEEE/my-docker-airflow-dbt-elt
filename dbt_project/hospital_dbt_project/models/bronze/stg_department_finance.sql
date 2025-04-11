WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'department_finance')
    
}}
)

SELECT
   	"c1" AS department_financial_id,
	"c2" AS department_id,
	"c3" AS department_name,
	"c4" AS branch_name,
	"c5" AS state_name,
	"c6" AS year_month,
	"c7" AS revenue,
	"c8" AS expenses,
	"c9" AS total_claims_submitted,
	"c10" AS insurance_claims_approved,
	"c11" AS cost_per_patient,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source