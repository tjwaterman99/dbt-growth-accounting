{{
    config(materialized='table')
}}

with active_days as (
    select 
        object_type,
        object_id,
        active_at,  -- this is cast to a 'date' type
        count(1) as events
    from {{ ref('growth_accounting_events__stg') }}
    group by 1,2,3
)

select
    active_days.*,
    extract('day' from active_at)::integer as active_at_day,
    extract('month' from active_at)::integer as active_at_month,
    extract('year' from active_at)::integer as active_at_year,
    extract('doy' from active_at)::integer as active_at_day_of_year,
    extract('dow' from active_at)::integer as active_at_day_of_week,
    extract('quarter' from active_at)::integer as active_at_quarter,
    extract('week' from active_at)::integer as active_at_week_of_year
from active_days    