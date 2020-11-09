{{
    config(materialized='table')
}}

with daily_activity_enhanced as (
    select 
        daily_activity.object_type,
        daily_activity.event_type,
        daily_activity.object_id,
        daily_activity.active_at,
        daily_activity.events,
        cohorts.first_active_at,
        daily_activity.active_at - lag(daily_activity.active_at) over (partition by daily_activity.object_type, daily_activity.event_type, daily_activity.object_id order by daily_activity.active_at asc) as days_since_last_active
    from {{ ref('growth_accounting_daily_activity__stg') }} daily_activity
    join {{ ref('growth_accounting_cohorts') }} cohorts
        on daily_activity.object_type = cohorts.object_type
        and daily_activity.event_type = cohorts.event_type
        and daily_activity.object_id = cohorts.object_id
),

daily_activity_with_new_user_flags as (
    select
        daily_activity_enhanced.*,
        case when daily_activity_enhanced.active_at - daily_activity_enhanced.first_active_at = 0 then true else false end as is_1d_new,
        case when daily_activity_enhanced.active_at - daily_activity_enhanced.first_active_at < 7 then true else false end as is_7d_new,
        case when daily_activity_enhanced.active_at - daily_activity_enhanced.first_active_at < 30 then true else false end as is_30d_new,
        case when daily_activity_enhanced.active_at - daily_activity_enhanced.first_active_at < 90 then true else false end as is_90d_new,
        case when daily_activity_enhanced.active_at - daily_activity_enhanced.first_active_at < 365 then true else false end as is_365d_new
    from daily_activity_enhanced
),

daily_activity_with_retained_user_flags as (
    select 
        daily_activity_with_new_user_flags.*,
        case when not is_1d_new and daily_activity_with_new_user_flags.days_since_last_active = 1 then true else false end as is_1d_retained,
        case when not is_7d_new and daily_activity_with_new_user_flags.days_since_last_active < 7 then true else false end as is_7d_retained,
        case when not is_30d_new and daily_activity_with_new_user_flags.days_since_last_active < 30 then true else false end as is_30d_retained,
        case when not is_90d_new and daily_activity_with_new_user_flags.days_since_last_active < 90 then true else false end as is_90d_retained,
        case when not is_365d_new and daily_activity_with_new_user_flags.days_since_last_active < 365 then true else false end as is_365d_retained
    from daily_activity_with_new_user_flags
),

daily_activity_with_returned_user_flags as (
    select
        daily_activity_with_retained_user_flags.*,
        not is_1d_new and not is_1d_retained as is_1d_returned,
        not is_7d_new and not is_7d_retained as is_7d_returned,
        not is_30d_new and not is_30d_retained as is_30d_returned,
        not is_90d_new and not is_90d_retained as is_90d_returned,
        not is_365d_new and not is_365d_retained as is_365d_returned
    from daily_activity_with_retained_user_flags
)

select * from daily_activity_with_returned_user_flags
