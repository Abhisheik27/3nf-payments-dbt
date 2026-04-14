{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'refunds']
) }}

with source_data as (
    select
        refund_id,
        original_transaction_id,
        amount,
        reason,
        status,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'refunds') }}
)

select
    source_data.refund_id,
    source_data.original_transaction_id,
    source_data.amount,
    source_data.reason,
    source_data.status,
    case
        when source_data.status = 'completed' then 1
        else 0
    end as is_completed,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.refund_id is not null