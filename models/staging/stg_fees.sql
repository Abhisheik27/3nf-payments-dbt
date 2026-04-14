{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'fees']
) }}

with source_data as (
    select
        fee_id,
        transaction_id,
        amount,
        fee_type,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'fees') }}
)

select
    source_data.fee_id,
    source_data.transaction_id,
    source_data.amount,
    source_data.fee_type,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.fee_id is not null