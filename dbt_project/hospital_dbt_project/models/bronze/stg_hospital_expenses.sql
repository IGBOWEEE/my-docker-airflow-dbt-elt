WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'hospital_expenses')
    
}}
)

SELECT  
    "c1" AS expense_id,
	"c2" AS branch_name,
	"c3" AS state_name,
	"c4" AS expense_date,
	"c5" AS category,
	"c6" AS expense_amount,
	"c7" AS vendor_name,
	CURRENT_TIMESTAMP() AS _loaded_at

FROM source