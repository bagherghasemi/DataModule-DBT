{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_transactions') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    -- Use created_at if updated_at is not available
    where coalesce(updated_at, created_at) > (select max(coalesce(updated_at, created_at)) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as transaction_id,
        safe_cast(order_id as string) as order_id,
        safe_cast(gateway as string) as payment_gateway,

        -- Transaction classification (as reported by Shopify)
        safe_cast(kind as string) as kind,
        safe_cast(status as string) as status,
        safe_cast(test as boolean) as test,

        -- Monetary fields (as reported by Shopify, no calculations)
        safe_cast(currency as string) as currency,
        safe_cast(amount as numeric) as amount,
        safe_cast(authorization_code as string) as authorization_code,

        -- Timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(processed_at as timestamp) as processed_at,

        -- Payment metadata
        safe_cast(source_name as string) as source_name,
        receipt,
        safe_cast(error_code as string) as error_code,
        safe_cast(gateway_reference as string) as gateway_reference,

        -- Shopify metadata
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by transaction_id 
                order by coalesce(updated_at, created_at) desc
            ) as row_num
        from renamed
    )
    where row_num = 1

)

select
    -- Remove row_num helper column
    * except(row_num)

from deduped