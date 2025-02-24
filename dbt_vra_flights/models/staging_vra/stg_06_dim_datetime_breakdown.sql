{{
    config(
		materialized='table'
	)
}}


with q1 as (
    select
        sk_id,
    	date_part('day', scheduled_departure) as scheduled_departure_day,
    	date_part('month', scheduled_departure) as scheduled_departure_month,
    	date_part('year', scheduled_departure) as scheduled_departure_year,
    	date_part('dow', scheduled_departure) as scheduled_departure_dow,
    	to_char(scheduled_departure, 'Month') as scheduled_departure_month_name,
    	to_char(scheduled_departure, 'Day') as scheduled_departure_day_name,
    	scheduled_departure::date as scheduled_departure_date,

    -- actual departure timestamp breakdown
    	date_part('day', actual_departure) as actual_departure_day,
    	date_part('month', actual_departure) as actual_departure_month,
    	date_part('year', actual_departure) as actual_departure_year,
    	date_part('dow', actual_departure) as actual_departure_dow,
    	to_char(actual_departure, 'Month') as actual_departure_month_name,
    	to_char(actual_departure, 'Day') as actual_departure_day_name,
    	actual_departure::date as actual_departure_date,

    -- scheduled arrival timestamp breakdown
    	date_part('day', scheduled_arrival) as scheduled_arrival_day,
    	date_part('month', scheduled_arrival) as scheduled_arrival_month,
    	date_part('year', scheduled_arrival) as scheduled_arrival_year,
    	date_part('dow', scheduled_arrival) as scheduled_arrival_dow,
    	to_char(scheduled_arrival, 'Month') as scheduled_arrival_month_name,
    	to_char(scheduled_arrival, 'Day') as scheduled_arrival_day_name,
    	scheduled_arrival::date as scheduled_arrival_date,

    -- actual arrival timestamp breakdown
    	date_part('day', actual_arrival) as actual_arrival_day,
    	date_part('month', actual_arrival) as actual_arrival_month,
    	date_part('year', actual_arrival) as actual_arrival_year,
    	date_part('dow', actual_arrival) as actual_arrival_dow,
    	to_char(actual_arrival, 'Month') as actual_arrival_month_name,
    	to_char(actual_arrival, 'Day') as actual_arrival_day_name,
    	actual_arrival::date as actual_arrival_date	
from {{ref('stg_05_dim_datetime')}}
)

select *
from q1

