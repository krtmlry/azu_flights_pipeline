{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook="truncate table staging_vra.stg_08_fact_azu_flights"
    )
}}

with q1 as (
    select *
    from {{ ref('stg_08_fact_azu_flights') }}
)

select *
from q1