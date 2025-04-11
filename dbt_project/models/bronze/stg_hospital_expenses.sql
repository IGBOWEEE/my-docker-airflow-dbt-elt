WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'hospital_expenses')
    
}}
)

SELECT  
    "Expense ID" AS expense_id,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Date" AS expense_date,
	"Category" AS category,
	"Amount" AS expense_amount,
	"Vendor" AS vendor_name,
	CURRENT_TIMESTAMP() AS _loaded_at

FROM source