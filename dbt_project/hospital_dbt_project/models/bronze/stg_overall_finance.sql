WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'overall_finance')
    
}}
)

SELECT
    "c1" AS finance_id,
	"c2" AS branch_name,
	"c3" AS state_name,
	"c4" AS date,
	"c5" AS total_revenue,
	"c6" AS total_expenses,
	"c7" AS profitloss,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
