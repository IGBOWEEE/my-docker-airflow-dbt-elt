{{config(materialized='table', schema='gold', alias='dim_date')}}

-- description: this is a ddate dimension table covering the relevant period plus some buffer

WITH date_series AS (
    SELECT DATEADD(day, seq4(), '2018-01-01'):: DATE AS full_date
    FROM TABLE(GENERATOR(rowcount => (365 * 10))) -- 10 years of data
)

SELECT
    d.full_date,
    YEAR(d.full_date) AS year,
    QUARTER(d.full_date) AS quarter,
    MONTH(d.full_date) AS month,
    MONTHNAME(d.full_date) AS month_name,
    DAY(d.full_date) AS day_of_month,
    DAYOFWEEK(d.full_date) AS day_of_week,
    DAYNAME(d.full_date) AS day_name,
    WEEKOFYEAR(d.full_date) AS week_of_year,
    DAYOFYEAR(d.full_date) AS day_of_year,
    DATE_TRUNC('WEEK', d.full_date)::DATE AS week_start_date,
    DATE_TRUNC('MONTH', d.full_date)::DATE AS month_start_date,
    DATE_TRUNC('QUARTER', d.full_date)::DATE AS quarter_start_date,
    DATE_TRUNC('YEAR', d.full_date)::DATE AS year_start_date,
    CASE
        WHEN DAYOFWEEK(d.full_date) IN (0,6) THEN TRUE --Sunday, Saturday
        ELSE FALSE
    END AS is_weekend
    FROM date_series d
    WHERE d.full_date <= CURRENT_DATE() -- Filter to include only dates up to today
    ORDER BY d.full_date 


   