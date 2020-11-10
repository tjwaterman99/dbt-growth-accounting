

  create  table "dev"."public"."growth_accounting_by_month_draft__dbt_tmp"
  as (
    

with monthly_activity_rollup as (
    select 
        object_type,
        event_type,
        object_id,
        active_at_month,
        active_at_year,
        sum(events) as events
    from "dev"."public"."growth_accounting_activity"
    group by 1,2,3,4,5
),

dates as (
    select distinct
        extract(year from date_day::date)::integer as year,
        extract(month from date_day::date)::integer as month
    from "dev"."public"."growth_accounting_dates"
),

cohorts as (
    select * from "dev"."public"."growth_accounting_cohorts"
),

eligible_dates as (
    select 
        dates.year,
        dates.month,
        cohorts.first_active_at_year,
        cohorts.first_active_at_month,
        cohorts.object_id,
        cohorts.object_type,
        cohorts.event_type
    from dates 
    left join cohorts on dates.year >= cohorts.first_active_at_year 
        and dates.month >= cohorts.first_active_at_month
),

eligible_dates_with_active_flag as (
    select
        eligible_dates.*,
        case when monthly_activity_rollup.active_at_month is null then false else true end as is_active
    from eligible_dates
    left join monthly_activity_rollup
    on eligible_dates.year = monthly_activity_rollup.active_at_year
        and eligible_dates.month = monthly_activity_rollup.active_at_month
        and eligible_dates.object_id = monthly_activity_rollup.object_id
        and eligible_dates.event_type = monthly_activity_rollup.event_type
        and eligible_dates.object_type = monthly_activity_rollup.object_type
),

eligible_dates_with_active_groups as (
    select
        *,
        sum(is_active::int) over (partition by object_type, object_id, event_type order by "year" asc, "month" asc rows between unbounded preceding and current row) last_active_window
    from eligible_dates_with_active_flag
),

eligible_dates_with_last_active_dates as (
    select
        *,
        first_value(year) over (partition by last_active_window, object_type, object_id, event_type order by "year" asc, "month") as last_active_year,
        first_value(month) over (partition by last_active_window, object_type, object_id, event_type order by "year" asc, "month") as last_active_month
    from eligible_dates_with_active_groups
),

eligible_dates_with_months_since_last_active as (
    select
        *,
        12 * (year - last_active_year) + (month - last_active_month) as months_since_last_active
    from eligible_dates_with_last_active_dates
)

select * 
from eligible_dates_with_months_since_last_active
  );