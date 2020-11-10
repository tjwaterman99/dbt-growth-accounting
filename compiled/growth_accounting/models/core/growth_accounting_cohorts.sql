

with cohorts as (
    select
        object_type,
        event_type,
        object_id,
        max(active_at) as last_active_at,
        min(active_at) as first_active_at,
        count(1) as num_active_days,
        count(distinct(active_at_week::varchar || '-' || active_at_year::varchar)) as num_active_weeks,
        count(distinct(active_at_month::varchar || '-' || active_at_year::varchar)) as num_active_months,
        count(distinct(active_at_quarter::varchar || '-' || active_at_year::varchar)) as num_active_quarters,
        count(distinct(active_at_year)) as num_active_years,
        sum(events) as lifetime_events
    from "dev"."public"."growth_accounting_activity"
    group by 1, 2, 3
)

select
    *,
    extract('day' from first_active_at)::integer as first_active_at_day,
    extract('month' from first_active_at)::integer as first_active_at_month,
    extract('year' from first_active_at)::integer as first_active_at_year,
    extract('doy' from first_active_at)::integer as first_active_at_day_of_year,
    extract('dow' from first_active_at)::integer as first_active_at_day_of_week,
    extract('quarter' from first_active_at)::integer as first_active_at_quarter,
    extract('week' from first_active_at)::integer as first_active_at_week
    -- last_active_at - first_active_at as lifetime_age_in_days,
    -- (last_active_at - first_active_at) / 7 as lifetime_age_in_weeks,
    -- (last_active_at - first_active_at) / 30 as lifetime_age_in_months,
    -- (last_active_at - first_active_at) / 365 as lifetime_age_in_years
from cohorts