-- description: this model joins billing transactions with paitents
-- we essentially are referencing the stg_billing and dim patients to create a joined table
WITH stg_billing AS (
    SELECT * FROM {{ ref('stg_billing') }}
),
dim_patients AS (
    SELECT
        patient_id,
        patient_name,
        insurance_status,
        insurance_provider
    FROM {{ ref('dim_patients') }}
),
joined AS (
    SELECT  
        pat.patient_id,
        pat.patient_name,
        pat.insurance_status,
        pat.insurance_provider,
        bill.billing_id,
        bill.billing_date,
        bill.insurance_covered,
        bill.out_of_pocket,
        bill.payment_method,
        bill.state_name,
        bill.branch_name,
        bill._loaded_at
    FROM stg_billing bill
    LEFT JOIN dim_patients pat 
        ON bill.patient_id = pat.patient_id
    WHERE bill.billing_id IS NOT NULL
    
    

)
    SELECT * FROM joined