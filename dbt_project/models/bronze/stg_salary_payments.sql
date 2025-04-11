WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'salary_payments')
    
}}
)

SELECT
    "Salary Payment ID" AS salary_payment_id,
	"Employee ID" AS employee_id,
	"Name" AS employee_name,
	"Department Name" AS employee_dept,
	"Branch Name" AS branch_name,
	"Payment Date" AS payment_date,
	"Amount Paid" AS amount_paid,
	"Payment Method" AS payment_method,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source