WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'overall_finance')
    
}}
)

SELECT
    "Finance ID" AS finance_id,
	"Branch Name" AS branch_name,
	"State" AS state_name,
	"Date" AS date,
	"Total Revenue" AS total_revenue,
	"Total Expenses" AS total_expenses,
	"Profit/Loss" AS profit_or_loss,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
