{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'transactions']
) }}

with source_data as (
    select
        transaction_id,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        status,
        created_at,
        cast(created_at as date) as created_date,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'transactions') }}
)

select
    source_data.transaction_id,
    source_data.debtor_customer_id,
    source_data.creditor_customer_id,
    source_data.payment_method_id,
    source_data.amount,
    source_data.currency,
    source_data.status,
    source_data.created_at,
    source_data.created_date,
    case
        when source_data.status = 'completed' then 1
        else 0
    end as is_completed,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.transaction_id is not null