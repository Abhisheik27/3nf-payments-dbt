{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'dimension', 'mandates'],
    unique_id='mandate_id'
) }}

-- 3NF Dimension: dim_mandate
-- Primary Key: mandate_id
-- Foreign Keys: customer_id -> dim_customer, payment_method_id -> dim_payment_method
-- Contains standing mandate attributes

with stg_mandates as (
    select * from {{ ref('stg_mandates') }}
)

select
    mandate_id,
    customer_id,
    payment_method_id,
    status,
    is_active,
    current_timestamp() as dbt_updated_at,
    cast(current_date() as timestamp) as dbt_valid_from
from stg_mandates
where mandate_id is not null