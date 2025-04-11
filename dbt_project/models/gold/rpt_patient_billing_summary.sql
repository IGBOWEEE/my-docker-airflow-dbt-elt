-- models/gold/rpt_patient_billing_summary.sql
{{ config(materialized='table', schema='gold') }}

WITH patient_billing AS (
    SELECT
        patient_id,
        billing_date,
        total_billing_amt,
        insurance_covered,
        out_of_pocket
    FROM {{ ref('stg_billing') }} 
    WHERE patient_id IS NOT NULL AND billing_date IS NOT NULL
)

SELECT
    pb.patient_id,
    p.patient_name, 
    p.patient_age,  
    p.patient_gender,
    p.insurance_provider, -- Get primary insurance from dimension
    p.branch_name AS primary_branch, -- Get primary branch from dimension

    COUNT(pb.billing_date) AS number_of_bills,
    SUM(pb.total_billing_amt) AS total_billed_amount,
    SUM(pb.insurance_covered) AS total_insurance_covered,
    SUM(pb.out_of_pocket) AS total_out_of_pocket,
    MIN(pb.billing_date) AS first_billing_date,
    MAX(pb.billing_date) AS last_billing_date,

    -- Calculate overall insurance coverage percentage
    IFF(SUM(pb.total_billing_amt) > 0,
        (SUM(pb.insurance_covered) * 100.0 / SUM(pb.total_billing_amt)),
        0
       ) AS overall_insurance_coverage_pct

FROM patient_billing pb
LEFT JOIN {{ ref('dim_patients') }} p -- Join with the Silver patient dimension
    ON pb.patient_id = p.patient_id
GROUP BY
    pb.patient_id,
    p.patient_name,
    p.patient_age,
    p.patient_gender,
    p.insurance_provider,
    p.branch_name
ORDER BY
    total_billed_amount DESC 