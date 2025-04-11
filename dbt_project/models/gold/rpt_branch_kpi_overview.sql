-- models/gold/rpt_branch_kpi_overview.sql
{{ config(materialized='table', schema='gold') }}

WITH monthly_admissions AS (
    SELECT
        DATE_TRUNC('month', admission_date)::DATE AS performance_month,
        branch_name,
        state_name,
        SUM(total_admissions) AS monthly_admissions,
        SUM(total_readmissions) AS monthly_readmissions,
        AVG(avg_length_of_stay_days) AS avg_monthly_los -- Average of daily averages
    FROM {{ ref('fct_daily_admissions_summary') }}
    GROUP BY 1, 2, 3
),

monthly_er AS (
    SELECT
        DATE_TRUNC('month', date)::DATE AS performance_month,
        branch_name,
        state_name,
        AVG(er_wait_time) AS avg_er_wait_time_minutes,
        SUM(patient_inflow) AS total_er_inflow,
        SUM(patient_outflow) AS total_er_outflow,
        AVG(treatment_success_rate_percentage) AS avg_treatment_success_rate_pct
    FROM {{ ref('stg_er_performance') }} --
    WHERE date IS NOT NULL
    GROUP BY 1, 2, 3
),

monthly_occupancy AS (
     SELECT
        DATE_TRUNC('month', date)::DATE AS performance_month,
        branch_name,
        state_name,
        AVG(occupancy_rate) AS avg_occupancy_rate_pct,
        AVG(total_beds) AS avg_total_beds,
        AVG(occupied_beds) AS avg_occupied_beds
     FROM {{ ref('stg_bed_occupancy') }} 
     WHERE date IS NOT NULL
     GROUP BY 1, 2, 3
),

monthly_overall_finance AS (
     SELECT
        DATE_TRUNC('month', date)::DATE AS performance_month,
        branch_name,
        state_name,
        SUM(total_revenue) AS total_revenue_overall,
        SUM(total_expenses) AS total_expenses_overall,
        SUM(profit_or_loss) AS total_profit_loss
     FROM {{ ref('stg_overall_finance') }} 
     WHERE date IS NOT NULL
     GROUP BY 1, 2, 3
)

SELECT
    d.full_date AS performance_month, -- Join with Date dimension to ensure all months are present
    COALESCE(adm.branch_name, er.branch_name, occ.branch_name, fin.branch_name) AS branch_name,
    COALESCE(adm.state_name, er.state_name, occ.state_name, fin.state_name) AS state_name,

    -- Admission Metrics
    ZEROIFNULL(adm.monthly_admissions) AS monthly_admissions,
    ZEROIFNULL(adm.monthly_readmissions) AS monthly_readmissions,
    adm.avg_monthly_los,

    -- ER Metrics
    er.avg_er_wait_time_minutes,
    ZEROIFNULL(er.total_er_inflow) AS total_er_inflow,
    ZEROIFNULL(er.total_er_outflow) AS total_er_outflow,
    er.avg_treatment_success_rate_pct,

    -- Occupancy Metrics
    occ.avg_occupancy_rate_pct,
    occ.avg_occupied_beds,
    occ.avg_total_beds,

    -- Financial Metrics
    ZEROIFNULL(fin.total_revenue_overall) AS total_revenue,
    ZEROIFNULL(fin.total_expenses_overall) AS total_expenses,
    ZEROIFNULL(fin.total_profit_loss) AS total_profit_loss

FROM {{ ref('dim_date') }} d
LEFT JOIN monthly_admissions adm
    ON d.full_date = adm.performance_month AND d.day_of_month = 1 -- Join on first day of month
LEFT JOIN monthly_er er
    ON d.full_date = er.performance_month AND d.day_of_month = 1
    AND adm.branch_name = er.branch_name -- Join also on branch
LEFT JOIN monthly_occupancy occ
    ON d.full_date = occ.performance_month AND d.day_of_month = 1
    AND COALESCE(adm.branch_name, er.branch_name) = occ.branch_name -- Join also on branch
LEFT JOIN monthly_overall_finance fin
    ON d.full_date = fin.performance_month AND d.day_of_month = 1
    AND COALESCE(adm.branch_name, er.branch_name, occ.branch_name) = fin.branch_name -- Join also on branch
WHERE
    d.day_of_month = 1 -- Only select the first day of each month from dim_date
    AND d.full_date >= '2018-01-01' AND d.full_date <= '2027-12-31' -- Filter date range relevant to data
    -- Ensure at least one metric exists for the row to avoid empty month rows if dim_date is much larger
    AND COALESCE(adm.branch_name, er.branch_name, occ.branch_name, fin.branch_name) IS NOT NULL
ORDER BY
    performance_month DESC,
    branch_name