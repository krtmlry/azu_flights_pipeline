{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='sk_id',
        post_hook= "truncate table staging_vra.stg_04_dim_justification_recs"
    )
}}

with q1 as (
    select
    *
    from {{ref('stg_04_dim_justification_recs')}}

)

select *
from q1