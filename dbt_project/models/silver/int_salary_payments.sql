WITH stg_salary_payments AS (
    SELECT * FROM {{ ref('stg_salary_payments') }}
),

dim_employees AS (
    SELECT
        employee_id,
        employee_name,
        job_role,
        department_name,
        state_name,
        branch_name
    FROM {{ ref('dim_employees') }}
),

joined AS (
    SELECT  
        emp.employee_id,
        emp.employee_name,
        emp.job_role,
        emp.department_name,
        sal.salary_payment_id,
        sal.payment_date,
        sal.amount_paid,
        sal.payment_method,
        emp.state_name,
        emp.branch_name,
        sal._loaded_at
    FROM stg_salary_payments sal
    LEFT JOIN dim_employees emp 
        ON sal.employee_id = emp.employee_id
    WHERE sal.salary_payment_id IS NOT NULL

)
SELECT * FROM joined

        
