{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'fact', 'fees'],
    unique_id='fee_id'
) }}

-- 3NF Fact: fact_fee
-- Primary Key: fee_id
-- Foreign Key: transaction_id -> fact_transaction
-- Contains fee transaction facts

with stg_fees as (
    select * from {{ ref('stg_fees') }}
),

stg_transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    f.fee_id,
    f.transaction_id,
    t.debtor_customer_id,
    t.creditor_customer_id,
    f.amount as fee_amount,
    f.fee_type,
    t.currency,
    current_timestamp() as dbt_loaded_at
from stg_fees f
left join stg_transactions t on f.transaction_id = t.transaction_id
where f.fee_id is not null