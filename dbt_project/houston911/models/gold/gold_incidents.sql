{{ config(
    materialized='table'
) }}

with base as (
    select 
        Agency,
        date_trunc('day', call_time) as call_date,
        count(distinct UID) as total_incidents,
        sum(case when CombinedResponse = 'F' then 1 else 0 end) as false_alarms,
        sum(case when CombinedResponse = 'P' then 1 else 0 end) as primary_responses
    from {{ ref('silver_incidents') }}
    group by 1, 2
),

enhanced as (
    select 
        *,
        round((false_alarms::decimal / total_incidents) * 100, 2) as false_alarm_rate
    from base
)

select * from enhanced
