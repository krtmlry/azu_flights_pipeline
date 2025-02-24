{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook= "truncate table staging_vra.stg_03_convert_types_keys"
    )
}}

with q1 as (
    select *
    from {{ref('stg_03_convert_types_keys')}}
)

select *
from q1