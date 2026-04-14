{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'transaction_legs']
) }}

with source_data as (
    select
        transaction_leg_id,
        transaction_id,
        leg_sequence,
        amount,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'transaction_legs') }}
)

select
    source_data.transaction_leg_id,
    source_data.transaction_id,
    source_data.leg_sequence,
    source_data.amount,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.transaction_leg_id is not null