{{ config(
    materialized='table',
    schema='intermediate',
    tags=['silver', 'fact', 'refunds'],
    unique_id='refund_id'
) }}

-- 3NF Fact: fact_refund
-- Primary Key: refund_id
-- Foreign Key: original_transaction_id -> fact_transaction
-- Contains refund transaction facts

with stg_refunds as (
    select * from {{ ref('stg_refunds') }}
),

stg_transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    r.refund_id,
    r.original_transaction_id,
    t.debtor_customer_id,
    t.creditor_customer_id,
    r.amount as refund_amount,
    r.reason as refund_reason,
    r.status as refund_status,
    r.is_completed,
    t.currency,
    current_timestamp() as dbt_loaded_at
from stg_refunds r
left join stg_transactions t on r.original_transaction_id = t.transaction_id
where r.refund_id is not null