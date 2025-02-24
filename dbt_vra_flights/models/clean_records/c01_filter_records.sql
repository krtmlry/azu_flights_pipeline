{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook= "truncate table staging_vra.stg_01_filter_records"
    )
}}

with q1 as(
    select *
    from {{ref('stg_01_filter_records')}}
)

select *
from q1