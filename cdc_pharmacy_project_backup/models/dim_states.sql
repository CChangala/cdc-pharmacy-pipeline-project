-- models/dim_states.sql

select
    state_code,
    state_name
from {{ ref('state_abbreviations') }}