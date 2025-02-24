{{
    config(
        materialized='table'
    
    )
}}

-- Convert data types, remove duplicates, add surrogate_keys

with q1 as (
    select
        airline_icao::varchar                                           as airline_icao,
        flight_number::varchar                                          as flight_number,
        line_type_code::char                                            as line_type_code,
        aircraft_icao::varchar                                          as aircraft_icao,
        passenger_count::int                                            as passenger_count,
        orig_airport_icao::varchar                                      as orig_airport_icao,
        to_timestamp(scheduled_departure, 'DD/MM/YYYY HH24:MI')         as scheduled_departure,
        to_timestamp(mod_actual_departure, 'DD/MM/YYYY HH24:MI')        as actual_departure,
        dest_airport_icao::varchar                                      as dest_airport_icao,
        to_timestamp(scheduled_arrival, 'DD/MM/YYYY HH24:MI')           as scheduled_arrival,
        to_timestamp(actual_arrival, 'DD/MM/YYYY HH24:MI')              as actual_arrival,
        case
            when flight_status = 'CANCELADO' then 'Cancelled'
            else 'Accomplished'
        end                                                             as flight_status,
        initcap(coalesce(justification,'No justification'))::varchar 	as justification,
        mod_flag::int                                                   as mod_flag
    from {{ ref('stg_02_modify_datetime') }}
    order by scheduled_departure asc    
),

-- Select unique records based on all columns
q2 as (
    select
        distinct *
    from q1
    order by scheduled_departure asc    
),

-- Generate / add surrogate key id
q3 as (
    select
        md5(airline_icao||flight_number||scheduled_departure||scheduled_arrival||row_number() over()) as sk_id,
        *
    from q2
    order by scheduled_departure asc 
)

select *
from q3