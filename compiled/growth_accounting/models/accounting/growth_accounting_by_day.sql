

with dates as (
    select date_day::date as date
    from "dev"."public"."growth_accounting_dates"
),

cohorts as (
    select * from "dev"."public"."growth_accounting_cohorts"
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
    left join "dev"."public"."growth_accounting_activity" daily_activity
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
        first_active_at = date as is_first_active,
        date - first_value(active_at) over (partition by ew.object_id,
                                                ew.event_type,
                                                ew.object_type,
                                                ew.active_at_window) as days_since_last_active
    from eligible_dates_with_active_at_window ew
),

activity_by_day_with_new_churned_flags as (
    select
        activity_by_day.*,
        case when date - first_active_at < 1 then true else false end as is_1d_new,
        case when date - first_active_at < 7 then true else false end as is_7d_new,
        case when date - first_active_at < 30 then true else false end as is_30d_new,
        case when date - first_active_at < 90 then true else false end as is_90d_new,
        case when date - first_active_at < 365 then true else false end as is_365d_new,

        -- Churned flags
        case when days_since_last_active >= 1 then true else false end as is_1d_churned,
        case when days_since_last_active >= 7 then true else false end as is_7d_churned,
        case when days_since_last_active >= 30 then true else false end as is_30d_churned,
        case when days_since_last_active >= 90 then true else false end as is_90d_churned,
        case when days_since_last_active >= 365 then true else false end as is_365d_churned
    from activity_by_day
),

activity_by_day_with_returned_flags as (
    select
        *,
        case when lag(is_1d_churned) over cohort and is_active then true else false end as is_1d_returned,
        case when lag(is_7d_churned) over cohort and is_active then true else false end as is_7d_returned,
        case when lag(is_30d_churned) over cohort and is_active then true else false end as is_30d_returned,
        case when lag(is_90d_churned) over cohort and is_active then true else false end as is_90d_returned,
        case when lag(is_365d_churned) over cohort and is_active then true else false end as is_365d_returned
    from activity_by_day_with_new_churned_flags
    window cohort as (partition by object_id, object_type, event_type order by date asc)
),

activity_by_day_with_all_flags as (
    select
        activity_by_day_with_returned_flags.*,
        case when days_since_last_active < 1 and not is_1d_returned and not is_1d_new then true else false end as is_1d_retained,
        case when days_since_last_active < 7 and not is_7d_returned and not is_7d_new then true else false end as is_7d_retained,
        case when days_since_last_active < 30 and not is_30d_returned and not is_30d_new then true else false end as is_30d_retained,
        case when days_since_last_active < 90 and not is_90d_returned and not is_90d_new then true else false end as is_90d_retained,
        case when days_since_last_active < 365 and not is_365d_returned and not is_365d_new then true else false end as is_365d_retained
    from activity_by_day_with_returned_flags
)

select
    *,
    case
        when is_1d_new then 'new'
        when is_1d_churned then 'churned'
        when is_1d_retained then 'retained'
        when is_1d_returned then 'returned'
        else null
    end as status_1d,
    case
        when is_7d_new then 'new'
        when is_7d_churned then 'churned'
        when is_7d_retained then 'retained'
        when is_7d_returned then 'returned'
        else null
    end as status_7d,
    case
        when is_30d_new then 'new'
        when is_30d_churned then 'churned'
        when is_30d_retained then 'retained'
        when is_30d_returned then 'returned'
        else null
    end as status_30d,
    case
        when is_90d_new then 'new'
        when is_90d_churned then 'churned'
        when is_90d_retained then 'retained'
        when is_90d_returned then 'returned'
        else null
    end as status_90d,
    case
        when is_365d_new then 'new'
        when is_365d_churned then 'churned'
        when is_365d_retained then 'retained'
        when is_365d_returned then 'returned'
        else null
    end as status_365d    
from activity_by_day_with_all_flags