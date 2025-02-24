{{
    config(
        materialized='table'
    )
}}

with q1 as (
    select
        sk_id,
        justification
    from {{ref('stg_03_convert_types_keys')}}
)

select *
from q1
order by sk_id asc