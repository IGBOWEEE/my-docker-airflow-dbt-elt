WITH source AS (
    SELECT * FROM {{ source('raw_hospital_data', 'employees') }}
)

SELECT
  "Employee ID" AS employee_id,
  "Name" AS name,
  "Age" AS age,
  "Gender" AS gender,
  "Phone" AS emp_phone_number,
  "Email" AS emp_email,
  "Department ID" AS department_id,
  "Department Name" AS department_id,
  "Branch Name" AS branch_name,
  "State" AS state,
  "Role" AS job_role,
  CURRENT_TIMESTAMP() AS _loaded_at

FROM source









