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
        case when growth_accounting_activity_by_day.active_at is null then false else true end as is_active,
        -- Ideally we could just do `lag(active_at) ignore_nulls (partition by ...)` to get the last
        -- active date
        active_at
    from eligible_dates
    left join growth_accounting_activity_by_day
    on eligible_dates.date = growth_accounting_activity_by_day.active_at
        and eligible_dates.object_id = growth_accounting_activity_by_day.object_id
        and eligible_dates.event_type = growth_accounting_activity_by_day.event_type
        and eligible_dates.object_type = growth_accounting_activity_by_day.object_type
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
)

select
    date,
    object_id,
    object_type,
    event_type,
    is_active,
    first_active_at,
    first_value(active_at) over (partition by ew.object_id,
                                              ew.event_type,
                                              ew.object_type,
                                              ew.active_at_window) as last_active_at
from eligible_dates_with_active_at_window ew                                          