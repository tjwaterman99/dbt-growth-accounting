{{ config(
        materialized='table'
    )
}}

with cohorts as (
    select
        object_type,
        object_id,
        max(active_at) as last_active_at,
        min(active_at) as first_active_at,
        sum(events) as lifetime_events
    from {{ ref('growth_accounting_active_dates') }}
    group by 1, 2
)

select *
from cohorts