{{
    config(materialized='table')
}}

select 
    *,
    active_at - lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) as days_since_last_active,
    case when lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) is null then true else false end as is_new,
    case when active_at - lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) <= 1 then true else false end as is_retained,
    case when active_at - lag(active_at) over (partition by object_type, event_type, object_id order by active_at asc) > 1 then true else false end as is_returned
from {{ ref('growth_accounting_daily_activity') }}