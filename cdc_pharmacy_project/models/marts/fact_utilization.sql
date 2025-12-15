-- models/marts/fact_utilization.sql

with utilization as (
    select * from {{ ref('stg_cms_utilization') }}
),

states as (
    select * from {{ ref('dim_states') }}
)

select
    -- Foreign Keys (FKs) for joining to Dimension tables
    utilization.state_code,
    -- utilization.product_ndc,  -- Can be FK to a Product Dimension (if built)

    -- Attributes from the State Dimension
    states.state_name,

    -- Facts / Measures
    utilization.units_reimbursed,
    utilization.total_amount_reimbursed,
    utilization.medicaid_amount_reimbursed,
    utilization.non_medicaid_amount_reimbursed,

    -- Time Keys
    utilization.year,
    utilization.quarter

from utilization
left join states
    on utilization.state_code = states.state_code