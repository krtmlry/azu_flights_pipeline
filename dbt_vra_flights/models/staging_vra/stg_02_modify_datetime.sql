{{
    config(
        materialized='table'
    )
}}

with q1 as (
    select
        *,
        case
            when actual_departure is Null and flight_status = 'REALIZADO' then scheduled_departure
            else actual_departure
        end as mod_actual_departure,
        case
            when actual_arrival is Null and flight_status = 'REALIZADO' then scheduled_arrival
            else actual_arrival
        end as mod_actual_arrival,
        case
            when actual_departure is Null and actual_arrival is Null and flight_status != 'CANCELADO' then 1
        else 0
        end as mod_flag
    from {{ ref('stg_01_filter_records') }}
    order by scheduled_departure asc
)


select *
from q1