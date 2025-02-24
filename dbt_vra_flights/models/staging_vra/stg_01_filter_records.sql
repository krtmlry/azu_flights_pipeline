{{ 
    config(
        materialized='table'
    
    )
}}

-- Filter records to flights done by AZU and di_code = 0 which are regular flights
with q1 as (
    select *
    from {{source('vra_raw_records','vra_raw_landing')}}
    where trim(airline_icao) = 'AZU' 
      and trim(di_code) = '0'
      and trim(flight_number) IS NOT NULL 
      and trim(scheduled_departure) IS NOT NULL 
      and trim(scheduled_arrival) IS NOT NULL
),

-- Trim or remove leading and trailing spaces
q2 as (
        select
        trim(airline_icao) as airline_icao,
        trim(flight_number) as flight_number,
        trim(line_type_code) as line_type_code,
        trim(aircraft_icao) as aircraft_icao,
        trim(passenger_count) as passenger_count,
        trim(orig_airport_icao) as orig_airport_icao,
        trim(scheduled_departure) as scheduled_departure,
        trim(actual_departure) as actual_departure,
        trim(dest_airport_icao) as dest_airport_icao,
        trim(scheduled_arrival) as scheduled_arrival,
        trim(actual_arrival) as actual_arrival,
        trim(flight_status) as flight_status,
        trim(justification) as justification
    from q1
)

select 
distinct *
from q2