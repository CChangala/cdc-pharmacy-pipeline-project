-- models/staging/stg_cms_utilization.sql

with source as (
    select *
    from {{ source('raw_data', 'cms_utilization_raw') }} 
)

select
    -- Case-corrected to match BigQuery schema (State, NDC, Year, Quarter)
    cast(source.State as string) as state_code,
    cast(source.NDC as string) as product_ndc,

    -- Utilization & Cost Metrics
    cast(source.`Units Reimbursed` as float64) as units_reimbursed, 
    cast(source.`Total Amount Reimbursed` as float64) as total_amount_reimbursed, 
    cast(source.`Medicaid Amount Reimbursed` as float64) as medicaid_amount_reimbursed, 
    cast(source.`Non Medicaid Amount Reimbursed` as float64) as non_medicaid_amount_reimbursed, 

    -- Time
    cast(source.Year as int64) as year, 
    cast(source.Quarter as int64) as quarter 

from source