/*
I'm not really sure how to make the argument passing here more sensible without
researching how these macros actually work.
*/

{{
    config(materialized='table')
}}

{% set end_date = dbt_utils.get_query_results_as_dict('select max(active_at)::varchar end_date from ' ~ ref('growth_accounting_activity')) %}
{% set start_date = dbt_utils.get_query_results_as_dict('select min(active_at)::varchar start_date from ' ~ ref('growth_accounting_activity')) %}
{% set end_date_str = "'" ~ end_date['end_date'][0] ~ "'" %}
{% set start_date_str = "'" ~ start_date['start_date'][0] ~ "'" %}
{% set start_date_arg = "to_date(" ~ start_date_str ~ ", 'yyyy-mm-dd')" %}

{{
    dbt_utils.date_spine(
        datepart='day',
        start_date=start_date_arg,
        end_date=end_date_str
    )
}}