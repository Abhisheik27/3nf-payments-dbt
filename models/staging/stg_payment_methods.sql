{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'payment_methods']
) }}

with source_data as (
    select
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'payment_methods') }}
)

select
    source_data.payment_method_id,
    source_data.customer_id,
    source_data.method_type,
    source_data.is_default,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.payment_method_id is not null