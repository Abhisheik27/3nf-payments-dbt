{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'customers']
) }}

with source_data as (
    select
        customer_id,
        email,
        phone_number,
        kyc_status,
        risk_profile,
        created_at,
        cast(created_at as date) as created_date,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at
    from {{ source('payments_raw', 'customers') }}
)

select
    source_data.customer_id,
    source_data.email,
    source_data.phone_number,
    source_data.kyc_status,
    source_data.risk_profile,
    source_data.created_at,
    source_data.created_date,
    current_timestamp() as dbt_loaded_at
from source_data
where source_data.customer_id is not null