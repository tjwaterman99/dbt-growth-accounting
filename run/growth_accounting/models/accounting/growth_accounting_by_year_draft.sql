

  create  table "dev"."public"."growth_accounting_by_year_draft__dbt_tmp"
  as (
    

with yearly_rollup as (
    select 
        object_type,
        event_type,
        object_id,
        active_at_year,
        sum(events) as events
    from "dev"."public"."growth_accounting_activity"
    group by 1,2,3,4
),
    
yearly_rollup_enhanced as (
    select
        yearly_rollup.*,
        yearly_rollup.active_at_year - lag(yearly_rollup.active_at_year) over (partition by object_type, event_type, object_id order by active_at_year asc)
            as years_since_last_active 
    from yearly_rollup
)

select
    *,
    case when years_since_last_active is null then true else false end as is_new,
    case when years_since_last_active = 1 then true else false end as is_retained,
    case when years_since_last_active > 1 then true else false end as is_returned
from yearly_rollup_enhanced
  );