--description: this is a fact table derived from the stg_deprtment_fiancials table.
-- It contains financial data related to different departments in the hospital.

WITH stg_department_finance AS (
    SELECT * FROM {{ ref('stg_department_finance') }}
),
transformed AS (
    SELECT
        department_financial_id,
        department_id,
        --convert the month_year to date format
        TRY_TO_DATE(year_month || '01', 'YYYY-MM-DD') AS finance_month,
        revenue,
        expenses,
        total_claims_submitted,
        insurance_claims_approved,
        cost_per_patient,
        department_name,
        branch_name,
        state_name,
        _loaded_at
    FROM stg_department_finance
    WHERE TRY_TO_DATE(year_month || '01', 'YYYY-MM-DD') IS NOT NULL
        AND department_financial_id IS NOT NULL
)

    SELECT * FROM transformed