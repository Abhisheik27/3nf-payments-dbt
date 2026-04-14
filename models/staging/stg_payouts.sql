{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'payouts']
) }}

with source_data as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        status,
        scheduled_at,
        executed_at,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'payouts') }}
)

select
    source_data.payout_id,
    source_data.recipient_customer_id,
    source_data.amount,
    source_data.status,
    source_data.scheduled_at,
    source_data.executed_at,
    case
        when source_data.status = 'executed' then 1
        else 0
    end as is_executed,
    case
        when source_data.executed_at is not null
            then timestamp_diff(source_data.executed_at, source_data.scheduled_at, minute)
        else null
    end as execution_delay_minutes,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.payout_id is not null