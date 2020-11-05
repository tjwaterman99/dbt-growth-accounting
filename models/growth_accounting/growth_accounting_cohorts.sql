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
        count(1) as num_active_days,
        count(distinct(active_at_week_of_year::varchar || '-' || active_at_year::varchar)) as num_active_weeks,
        count(distinct(active_at_month::varchar || '-' || active_at_year::varchar)) as num_active_months,
        count(distinct(active_at_quarter::varchar || '-' || active_at_year::varchar)) as num_active_quarters,
        count(distinct(active_at_year)) as num_active_years,
        sum(events) as lifetime_events
    from {{ ref('growth_accounting_active_dates') }}
    group by 1, 2
)

select
    *,
    last_active_at - first_active_at as lifetime_age_in_days,
    (last_active_at - first_active_at) / 7 as lifetime_age_in_weeks,
    (last_active_at - first_active_at) / 30 as lifetime_age_in_months,
    (last_active_at - first_active_at) / 365 as lifetime_age_in_years
from cohorts