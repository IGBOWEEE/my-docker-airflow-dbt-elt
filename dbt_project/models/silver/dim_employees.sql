WITH stg_employees AS (
    SELECT *
    FROM {{ ref('stg_employees') }}
),
cleaned AS (
    SELECT
        employee_id,
        name AS employee_name,
        age AS employee_age,
        gender AS employee_gender,
        TRIM(REGEXP_REPLACE(emp_phone_number, '[^0-9]', '')) AS emp_phone_number,
        emp_email AS employee_email,
        department_id,
        department_name,
        branch_name,
        state as state_name,
        job_role,
        _loaded_at

    FROM stg_employees
)

    SELECT
        employee_id,
        employee_name,
        employee_age,
        employee_gender,
        emp_phone_number,
        employee_email,
        department_id,
        department_name,
        branch_name,
        state_name,
        job_role,
        _loaded_at
    FROM cleaned
    WHERE employee_id IS NOT NULL

