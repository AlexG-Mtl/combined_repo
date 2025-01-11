{{ config(
    materialized='incremental',
    unique_key='UID',
    on_schema_change='sync_all_columns'
) }}

with source_data as (
    select 
        UID,
        Agency,
        Address,
        CrossStreet,
        CAST(from_unixtime(CALL_TIME / 1000) AS timestamp) as call_time, -- Convert UNIX epoch time to timestamp
        CombinedResponse
    from {{ source('bronze', 'incidents') }}
),

cleaned_data as (
    select 
        UID,
        Agency,
        upper(Address) as address, -- Convert address to uppercase
        upper(CrossStreet) as cross_street, -- Convert cross street to uppercase
        call_time,
        CombinedResponse
    from source_data
)

select * from cleaned_data
