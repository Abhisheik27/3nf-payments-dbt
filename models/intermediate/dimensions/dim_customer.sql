{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'dimension', 'customers'],
    unique_id='customer_id'
) }}

-- 3NF Dimension: dim_customer
-- Primary Key: customer_id
-- Contains all customer attributes with no transactional data
-- One row per customer

with stg_customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    email,
    phone_number,
    kyc_status,
    risk_profile,
    created_at,
    created_date,
    -- Slowly Changing Dimension (SCD) Type 1 - No historical tracking
    current_timestamp() as dbt_updated_at,
    cast(current_date() as timestamp) as dbt_valid_from
from stg_customers
where customer_id is not null