

with quarterly_rollup as (
    select 
        object_type,
        event_type,
        object_id,
        active_at_quarter,
        active_at_year,
        sum(events) as events
    from "dev"."public"."growth_accounting_activity"
    group by 1,2,3,4,5
),
    
quarterly_rollup_enhanced as (
    select
        quarterly_rollup.*,
        4 * (quarterly_rollup.active_at_year - lag(quarterly_rollup.active_at_year) over (partition by object_type, event_type, object_id order by active_at_year asc, active_at_quarter asc))
            + 
        quarterly_rollup.active_at_quarter - lag(quarterly_rollup.active_at_quarter) over (partition by object_type, event_type, object_id order by active_at_year asc, active_at_quarter asc)
            as quarters_since_last_active 
    from quarterly_rollup
)

select
    *,
    case when quarters_since_last_active is null then true else false end as is_new,
    case when quarters_since_last_active = 1 then true else false end as is_retained,
    case when quarters_since_last_active > 1 then true else false end as is_returned
from quarterly_rollup_enhanced