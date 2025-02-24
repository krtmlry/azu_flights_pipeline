{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook="truncate table staging_vra.stg_06_dim_datetime_breakdown"

    )
}}


with q1 as (
    select
    *
    from {{ ref('stg_06_dim_datetime_breakdown') }}
)

select *
from q1