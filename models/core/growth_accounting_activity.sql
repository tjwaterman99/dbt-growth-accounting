{{
    config(materialized='table')
}}

with active_days as (
    select 
        object_type,
        event_type,
        object_id,
        active_at,  -- note this is cast to a 'date' type
        count(1) as events
    from {{ ref('growth_accounting_events') }}
    group by 1,2,3,4
),

active_days_enhanced as (
    select
        active_days.*,
        extract('day' from active_at)::integer as active_at_day,
        extract('month' from active_at)::integer as active_at_month,
        extract('year' from active_at)::integer as active_at_year,
        extract('doy' from active_at)::integer as active_at_day_of_year,
        extract('dow' from active_at)::integer as active_at_day_of_week,
        extract('quarter' from active_at)::integer as active_at_quarter,
        extract('week' from active_at)::integer as active_at_week
    from active_days
)

select * 
from active_days_enhanced