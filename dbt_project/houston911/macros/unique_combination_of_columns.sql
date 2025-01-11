{% macro test_unique_combination_of_columns(column_combinations) %}
with validation as (
    select
        {{ column_combinations | join(', ') }},
        count(*) as row_count
    from {{ this }}
    group by {{ column_combinations | join(', ') }}
    having count(*) > 1
)
select * from validation
{% endmacro %}


