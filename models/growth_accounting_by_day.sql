{{ config(materialized='table') }}

with dates as (
    select date_day::date as date
    from {{ ref('growth_accounting_active_dates') }}
),

cohorts as (
    select * from {{ ref('growth_accounting_cohorts') }}
),

eligible_dates as (
    select 
        dates.date,
        cohorts.first_active_at,
        cohorts.object_id,
        cohorts.object_type,
        cohorts.event_type
    from dates 
    left join cohorts on dates.date >= cohorts.first_active_at
),

eligible_dates_enhanced as (
    select
        eligible_dates.*,
        case when daily_activity.active_at is null then false else true end as is_active,
        -- Ideally we could just do `lag(active_at) ignore_nulls (partition by ...)` to get the last
        -- active date
        daily_activity.active_at
    from eligible_dates
    left join {{ ref('growth_accounting_daily_activity__stg') }} daily_activity
    on eligible_dates.date = daily_activity.active_at
        and eligible_dates.object_id = daily_activity.object_id
        and eligible_dates.event_type = daily_activity.event_type
        and eligible_dates.object_type = daily_activity.object_type
),

-- We only need to create this window manually on Postgres. Bigquery, Redshift, Snowflake
-- all support `lag(active_at) ignore nulls` to select the first previous non-null value,
-- but postgres does not.
eligible_dates_with_active_at_window as (
    select 
        *,
        count(active_at) over (partition by eligible_dates_enhanced.object_id,
                                                eligible_dates_enhanced.event_type,
                                                eligible_dates_enhanced.object_type
                                order by eligible_dates_enhanced.date asc
                                rows between unbounded preceding and current row) as active_at_window
    from eligible_dates_enhanced
),

activity_by_day as (
    select
        date,
        object_id,
        object_type,
        event_type,
        first_active_at,
        first_value(active_at) over (partition by ew.object_id,
                                                ew.event_type,
                                                ew.object_type,
                                                ew.active_at_window) as last_active_at,
        is_active,
        first_active_at = date as is_first_active
    from eligible_dates_with_active_at_window ew
)

select
    activity_by_day.*,
    "date" - last_active_at as days_since_last_active
from activity_by_day