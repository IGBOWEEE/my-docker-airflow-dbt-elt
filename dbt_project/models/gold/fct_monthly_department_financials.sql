{{ config(
    materialized='table',
    schema='gold',
    alias='fct_monthly_department_financials'
) }}

WITH dept_finance_stg AS (
    SELECT
        TRY_TO_DATE(year_month || '-01', 'YYYY-MM-DD') AS finance_month,
        department_id,
        department_name,
        branch_name,
        state_name,
        revenue,
        expenses,
        total_claims_submitted,
        insurance_claims_approved,
        cost_per_patient,
        -- Handle potential division by zero if claims submitted is 0
        IFF(total_claims_submitted > 0,
            (insurance_claims_approved * 100.0 / total_claims_submitted),
            0
           ) AS claim_approval_rate_pct
    FROM {{ ref('stg_department_finance') }} 
    WHERE TRY_TO_DATE(year_month || '-01', 'YYYY-MM-DD') IS NOT NULL -- Filter out invalid month formats
)

SELECT
    df.finance_month,
    df.department_id,
    df.department_name,
    df.branch_name,
    df.state_name,
    SUM(df.revenue) AS total_revenue,
    SUM(df.expenses) AS total_expenses,
    SUM(df.revenue - df.expenses) AS total_profit_loss,
    SUM(df.total_claims_submitted) AS total_claims_submitted,
    SUM(df.insurance_claims_approved) AS total_claims_approved,
    -- Average of the monthly approval rates (weighted avg might be better if needed)
    AVG(df.claim_approval_rate_pct) AS avg_monthly_claim_approval_rate_pct,
    -- Average of the monthly cost per patient
    AVG(df.cost_per_patient) AS avg_monthly_cost_per_patient
FROM dept_finance_stg df
GROUP BY
    df.finance_month,
    df.department_id,
    df.department_name,
    df.branch_name,
    df.state_name
ORDER BY
    df.finance_month DESC,
    df.branch_name,
    df.department_name