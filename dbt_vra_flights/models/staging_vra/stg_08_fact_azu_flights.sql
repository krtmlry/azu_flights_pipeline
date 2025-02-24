{{
    config(
        materialized='table'
    )
}}

with q1 as (
    select
        a.sk_id,
        a.flight_number,
        a.airline_icao,
        a.passenger_count,
        a.mod_flag,
        b1.airport_icao as orig_airport_icao,
        b2.airport_icao as dest_airport_icao,
        d.line_type_code,
        e.status_id as flight_status,
        f.departure_situation,
        f.arrival_situation,
        f.sk_id as flight_schedule_id
        
    from  {{ ref('stg_03_convert_types_keys')}} as a
    left join dim_facts.dim_airports            as b1   on a.orig_airport_icao = b1.airport_icao
    left join dim_facts.dim_airports            as b2   on a.dest_airport_icao = b2.airport_icao
    left join dim_facts.dim_line_types          as d    on a.line_type_code = d.line_type_code
    left join dim_facts.dim_flight_status       as e    on a.flight_status = e.description
    left join {{ref('stg_05_dim_datetime')}}    as f    on a.sk_id = f.sk_id
    
    order by a.sk_id asc
)

select *
from q1