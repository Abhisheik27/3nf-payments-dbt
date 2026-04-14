{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'dimension', 'payment_methods'],
    unique_id='payment_method_id'
) }}

-- 3NF Dimension: dim_payment_method
-- Primary Key: payment_method_id
-- Foreign Key: customer_id -> dim_customer
-- Contains payment method attributes with customer reference

with stg_payment_methods as (
    select * from {{ ref('stg_payment_methods') }}
)

select
    payment_method_id,
    customer_id,
    method_type,
    is_default,
    current_timestamp() as dbt_updated_at,
    cast(current_date() as timestamp) as dbt_valid_from
from stg_payment_methods
where payment_method_id is not null