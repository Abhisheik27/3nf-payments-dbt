{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'fact', 'payouts'],
    unique_id='payout_id'
) }}

-- 3NF Fact: fact_payout
-- Primary Key: payout_id
-- Foreign Key: recipient_customer_id -> dim_customer
-- Contains payout transaction facts

with stg_payouts as (
    select * from {{ ref('stg_payouts') }}
)

select
    payout_id,
    recipient_customer_id,
    amount as payout_amount,
    status as payout_status,
    is_executed,
    scheduled_at,
    executed_at,
    execution_delay_minutes,
    current_timestamp() as dbt_loaded_at
from stg_payouts
where payout_id is not null