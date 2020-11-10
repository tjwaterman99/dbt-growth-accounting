

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
        12 * (year - last_active_year) + (month - last_active_month) as months_since_last_active,
        12 * (year - first_active_at_year) + (month - first_active_at_month) as months_since_first_active
    from eligible_dates_with_last_active_dates
),

activity_by_month_with_new_churned_flags as (
    select
        eligible_dates_with_months_since_last_active.*,
        case when months_since_first_active < 1 then true else false end as is_1m_new,
        case when months_since_first_active < 3 then true else false end as is_3m_new,
        case when months_since_first_active < 6 then true else false end as is_6m_new,
        case when months_since_first_active < 12 then true else false end as is_12m_new,

        -- Churned flags
        case when months_since_last_active >= 1 then true else false end as is_1m_churned,
        case when months_since_last_active >= 3 then true else false end as is_3m_churned,
        case when months_since_last_active >= 6 then true else false end as is_6m_churned,
        case when months_since_last_active >= 12 then true else false end as is_12m_churned
    from eligible_dates_with_months_since_last_active
),

activity_by_month_with_returned_flags as (
    select
        *,
        case when lag(is_1m_churned) over cohort and is_active then true else false end as is_1m_returned,
        case when lag(is_3m_churned) over cohort and is_active then true else false end as is_3m_returned,
        case when lag(is_6m_churned) over cohort and is_active then true else false end as is_6m_returned,
        case when lag(is_12m_churned) over cohort and is_active then true else false end as is_12m_returned
    from activity_by_month_with_new_churned_flags
    window cohort as (partition by object_id, object_type, event_type order by year asc, month asc)
),

activity_by_month_with_all_flags as (
    select
        *,
        case when months_since_last_active < 1 and not is_1m_returned and not is_1m_new then true else false end as is_1m_retained,
        case when months_since_last_active < 3 and not is_3m_returned and not is_3m_new then true else false end as is_3m_retained,
        case when months_since_last_active < 6 and not is_6m_returned and not is_6m_new then true else false end as is_6m_retained,
        case when months_since_last_active < 12 and not is_12m_returned and not is_12m_new then true else false end as is_12m_retained
    from activity_by_month_with_returned_flags
)

select
    *,
    case
        when is_1m_new then 'new'
        when is_1m_churned then 'churned'
        when is_1m_retained then 'retained'
        when is_1m_returned then 'returned'
        else null
    end as status_1m,
    case
        when is_3m_new then 'new'
        when is_3m_churned then 'churned'
        when is_3m_retained then 'retained'
        when is_3m_returned then 'returned'
        else null
    end as status_3m,
    case
        when is_6m_new then 'new'
        when is_6m_churned then 'churned'
        when is_6m_retained then 'retained'
        when is_6m_returned then 'returned'
        else null
    end as status_6m,
    case
        when is_12m_new then 'new'
        when is_12m_churned then 'churned'
        when is_12m_retained then 'retained'
        when is_12m_returned then 'returned'
        else null
    end as status_12m   
from activity_by_month_with_all_flags