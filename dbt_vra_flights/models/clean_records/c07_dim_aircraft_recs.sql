{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook="truncate table staging_vra.stg_07_dim_aircraft_recs"

    )
}}


with q1 as (
    select
    *
    from {{ ref('stg_07_dim_aircraft_recs') }}
)

select *
from q1