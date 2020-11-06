{{
    config(materialized='table')
}}

with monthly_rollup as (
    select 
        object_type,
        event_type,
        object_id,
        active_at_month,
        active_at_year,
        sum(events) as events
    from {{ ref('growth_accounting_daily_activity') }}
    group by 1,2,3,4,5
),
    
monthly_rollup_enhanced as (
    select
        monthly_rollup.*,
        12 * (monthly_rollup.active_at_year - lag(monthly_rollup.active_at_year) over (partition by object_type, event_type, object_id order by active_at_year asc, active_at_month asc))
            + 
        monthly_rollup.active_at_month - lag(monthly_rollup.active_at_month) over (partition by object_type, event_type, object_id order by active_at_year asc, active_at_month asc)
            as months_since_last_active 
    from monthly_rollup
)

select
    *
    -- active_at - lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) as days_since_last_active,
    -- case when lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) is null then true else false end as is_new,
    -- case when active_at - lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) <= 1 then true else false end as is_retained,
    -- case when active_at - lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) > 1 then true else false end as is_returned
from monthly_rollup_enhanced