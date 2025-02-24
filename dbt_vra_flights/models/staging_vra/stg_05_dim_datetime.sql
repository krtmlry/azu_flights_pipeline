{{
    config(
        materialized='table'
    )
}}

-- Create a datetime table to store all date related records

with q1 as (
	select
    sk_id,
    scheduled_departure,
    actual_departure,
    coalesce(extract(epoch from (actual_departure - scheduled_departure)) / 60, 0)::numeric(10,2) 	as departure_time_diff,
    case
        when coalesce(extract(epoch from (actual_departure - scheduled_departure)) / 60, 0) < 0.0 then 'Advanced'
        when coalesce(extract(epoch from (actual_departure - scheduled_departure)) / 60, 0) <= 30.0 then 'Punctual'
        when coalesce(extract(epoch from (actual_departure - scheduled_departure)) / 60, 0) <= 60.0 then 'Delay 30-60'
        when coalesce(extract(epoch from (actual_departure - scheduled_departure)) / 60, 0) <= 120.0 then 'Delay 60-120'
        when coalesce(extract(epoch from (actual_departure - scheduled_departure)) / 60, 0) <= 240.0 then 'Delay 120-240'
        else 'Delay > 240'
    end as departure_situation,

    scheduled_arrival,
    actual_arrival,
    coalesce(extract(epoch from (actual_arrival - scheduled_arrival)) / 60, 0)::numeric(10,2) 		as arrival_time_diff,
	case
        when coalesce(extract(epoch from (actual_arrival - scheduled_arrival)) / 60, 0) < 0.0 then 'Advanced'
        when coalesce(extract(epoch from (actual_arrival - scheduled_arrival)) / 60, 0) <= 30.0 then 'Punctual'
        when coalesce(extract(epoch from (actual_arrival - scheduled_arrival)) / 60, 0) <= 60.0 then 'Delay 30-60'
        when coalesce(extract(epoch from (actual_arrival - scheduled_arrival)) / 60, 0) <= 120.0 then 'Delay 60-120'
        when coalesce(extract(epoch from (actual_arrival - scheduled_arrival)) / 60, 0) <= 240.0 then 'Delay 120-240'
        else 'Delay > 240'
    end as arrival_situation,
    coalesce(extract(epoch from (scheduled_arrival - scheduled_departure)) / 60, 0)::numeric(10,2) 	as expected_flight_duration,
    coalesce(extract(epoch from (actual_arrival - actual_departure)) / 60, 0)::numeric(10,2) 			as actual_flight_duration
	from {{ref('stg_03_convert_types_keys')}}
),

q2 as (
    select
        a.sk_id,
        a.scheduled_departure,
        a.actual_departure,
        a.departure_time_diff,           
        b1.situation_id as departure_situation,
        a.scheduled_arrival,
        a.actual_arrival,
        a.arrival_time_diff,
        b2.situation_id as arrival_situation,
        a.expected_flight_duration,
        a.actual_flight_duration
    from q1 as a
    left join dim_facts.dim_flight_situation as b1 on a.departure_situation = b1.situation_short_desc
    left join dim_facts.dim_flight_situation as b2 on a.arrival_situation = b2.situation_short_desc
)
    
select *
from q2
order by scheduled_departure asc
