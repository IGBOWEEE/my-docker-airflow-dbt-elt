WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'salary_payments')
    
}}
)

SELECT
    "c1" AS salary_payment_id,
	"c2" AS employee_id,
	"c3" AS employee_name,
	"c4" AS employee_dept,
	"c5" AS branch_name,
	"c6" AS payment_date,
	"c7" AS amount_paid,
	"c8" AS payment_method,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source