{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'fact', 'transactions'],
    unique_id='transaction_id'
) }}

-- 3NF Fact: fact_transaction
-- Primary Key: transaction_id
-- Foreign Keys: debtor_customer_id, creditor_customer_id, payment_method_id
-- Contains transactional facts with dimensional references

with stg_transactions as (
    select * from {{ ref('stg_transactions') }}
),

stg_fees as (
    select * from {{ ref('stg_fees') }}
),

-- Aggregate fees by transaction
fees_agg as (
    select
        transaction_id,
        sum(amount) as total_fees
    from stg_fees
    group by transaction_id
)

select
    t.transaction_id,
    t.debtor_customer_id,
    t.creditor_customer_id,
    t.payment_method_id,
    t.amount as transaction_amount,
    coalesce(f.total_fees, 0) as total_fees,
    t.amount - coalesce(f.total_fees, 0) as net_amount,
    t.currency,
    t.status,
    t.is_completed,
    t.created_at,
    t.created_date,
    current_timestamp() as dbt_loaded_at
from stg_transactions t
left join fees_agg f on t.transaction_id = f.transaction_id
where t.transaction_id is not null