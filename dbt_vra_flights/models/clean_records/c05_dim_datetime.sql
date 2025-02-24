{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook="truncate table staging_vra.stg_05_dim_datetime"
    )
}}

with q1 as (
    select
    *
    from {{ ref('stg_05_dim_datetime') }}
)

select *
from q1