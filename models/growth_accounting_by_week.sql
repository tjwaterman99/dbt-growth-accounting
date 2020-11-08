{{
    config(materialized='table')
}}

with weekly_rollup as (
    select 
        object_type,
        event_type,
        object_id,
        active_at_week_of_year,
        active_at_year,
        sum(events) as events
    from {{ ref('growth_accounting_daily_activity__stg') }}
    group by 1,2,3,4,5
),
    
weekly_rollup_enhanced as (
    select
        weekly_rollup.*,
        52 * (weekly_rollup.active_at_year - lag(weekly_rollup.active_at_year) over (partition by object_type, event_type, object_id order by active_at_year asc, active_at_week_of_year asc))
            + 
        weekly_rollup.active_at_week_of_year - lag(weekly_rollup.active_at_week_of_year) over (partition by object_type, event_type, object_id order by active_at_year asc, active_at_week_of_year asc)
            as weeks_since_last_active 
    from weekly_rollup
)

select
    *,
    case when weeks_since_last_active is null then true else false end as is_new,
    case when weeks_since_last_active = 1 then true else false end as is_retained,
    case when weeks_since_last_active > 1 then true else false end as is_returned
from weekly_rollup_enhanced