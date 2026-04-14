{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'disputes']
) }}

with source_data as (
    select
        dispute_id,
        transaction_id,
        reason,
        amount,
        status,
        created_at,
        cast(created_at as date) as created_date,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'disputes') }}
)

select
    source_data.dispute_id,
    source_data.transaction_id,
    source_data.reason,
    source_data.amount,
    source_data.status,
    source_data.created_at,
    source_data.created_date,
    case
        when source_data.status in ('won', 'resolved') then 1
        else 0
    end as is_resolved,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.dispute_id is not null