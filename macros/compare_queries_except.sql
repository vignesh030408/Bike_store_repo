{% macro compare_queries_except(a_query, b_query, primary_key=None) %}
with a as (
    {{ a_query }}
),

b as (
    {{ b_query }}
),

a_except_b as (
    select * from a
    {{ dbt_utils.except() }}
    select * from b
),

b_except_a as (
    select * from b
    {{ dbt_utils.except() }}
    select * from a
),

all_records as (
    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

)

    select
        in_a,
        in_b,
        count(*) as count
    from all_records
    group by 1, 2

{% endmacro %}