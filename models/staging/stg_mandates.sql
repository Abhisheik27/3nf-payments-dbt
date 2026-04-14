{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'mandates']
) }}

with source_data as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        status,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'mandates') }}
)

select
    source_data.mandate_id,
    source_data.customer_id,
    source_data.payment_method_id,
    source_data.status,
    case
        when source_data.status = 'active' then 1
        else 0
    end as is_active,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.mandate_id is not null