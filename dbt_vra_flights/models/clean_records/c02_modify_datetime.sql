{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook= "truncate table staging_vra.stg_02_modify_datetime"
    )
}}

with q1 as (
    select *
    from {{ref('stg_02_modify_datetime')}}
)

select *
from q1