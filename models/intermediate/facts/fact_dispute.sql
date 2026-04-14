{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'fact', 'disputes'],
    unique_id='dispute_id'
) }}

-- 3NF Fact: fact_dispute
-- Primary Key: dispute_id
-- Foreign Key: transaction_id -> fact_transaction
-- Contains dispute/chargeback facts

with stg_disputes as (
    select * from {{ ref('stg_disputes') }}
),

stg_transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    d.dispute_id,
    d.transaction_id,
    t.debtor_customer_id,
    t.creditor_customer_id,
    d.amount as dispute_amount,
    d.reason as dispute_reason,
    d.status as dispute_status,
    d.is_resolved,
    t.currency,
    d.created_at,
    d.created_date,
    current_timestamp() as dbt_loaded_at
from stg_disputes d
left join stg_transactions t on d.transaction_id = t.transaction_id
where d.dispute_id is not null