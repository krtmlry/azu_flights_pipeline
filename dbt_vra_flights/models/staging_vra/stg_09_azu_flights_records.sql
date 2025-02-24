{{
    config(
        materialized='table',
        post_hook='truncate table staging_vra.vra_raw_landing'
    )
}}

with q1 as (
    select
        a.sk_id,
        a.flight_number,
        a.passenger_count,
        a.mod_flag,
        a.orig_airport_icao,
        b1.airport_name as orig_airport_name,
        b1.country as orig_airport_country,
        a.dest_airport_icao,
        b2.airport_name as dest_airport_name,
        b2.country as dest_airport_country,
        d.description as line_type_desc,
        e.description as flight_status_desc,
        --departure details
        g.scheduled_departure,
        g.actual_departure,
        g.departure_time_diff,
        f1.situation_short_desc as departure_situation,
        h.scheduled_departure_day,
        h.scheduled_departure_month,
        h.scheduled_departure_year,
        h.scheduled_departure_dow,
        h.scheduled_departure_month_name,
        h.scheduled_departure_day_name,
        h.scheduled_departure_date,
        h.actual_departure_day,
        h.actual_departure_month,
        h.actual_departure_year,
        h.actual_departure_dow,
        h.actual_departure_month_name,
        h.actual_departure_day_name,
        h.actual_departure_date,        
        --arrival details
        g.scheduled_arrival,
        g.actual_arrival,
        g.arrival_time_diff,
        f2.situation_short_desc as arrival_situation,
        h.scheduled_arrival_day,
        h.scheduled_arrival_month,
        h.scheduled_arrival_year,
        h.scheduled_arrival_dow,
        h.scheduled_arrival_month_name,
        h.scheduled_arrival_day_name,
        h.scheduled_arrival_date,
        h.actual_arrival_day,
        h.actual_arrival_month,
        h.actual_arrival_year,
        h.actual_arrival_dow,
        h.actual_arrival_month_name,
        h.actual_arrival_day_name,
        h.actual_arrival_date,        
        
        --flight time calculation
        g.expected_flight_duration,
        g.actual_flight_duration
        
    from {{ref('stg_08_fact_azu_flights')}} as a
    left join dim_facts.dim_airports as b1 on a.orig_airport_icao = b1.airport_icao
    left join dim_facts.dim_airports as b2 on a.dest_airport_icao = b2.airport_icao
    left join dim_facts.dim_line_types as d on a.line_type_code = d.line_type_code
    left join dim_facts.dim_flight_status as e on a.flight_status = e.status_id
    left join dim_facts.dim_flight_situation as f1 on a.departure_situation = f1.situation_id
    left join dim_facts.dim_flight_situation as f2 on a.arrival_situation = f2.situation_id
    left join {{ref('stg_05_dim_datetime')}} as g on a.flight_schedule_id = g.sk_id
    left join {{ref('stg_06_dim_datetime_breakdown')}} as h on a.flight_schedule_id = h.sk_id

    
    order by g.scheduled_departure asc    
)

select *
from q1