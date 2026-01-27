{{
    config(
        materialized='incremental',
        unique_key='refund_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_refunds') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    -- Use created_at if updated_at is not available
    where coalesce(updated_at, created_at) > (select max(coalesce(updated_at, created_at)) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as refund_id,
        safe_cast(order_id as string) as order_id,
        safe_cast(user_id as string) as user_id,

        -- Timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(processed_at as timestamp) as processed_at,

        -- Refund amounts (as reported by Shopify, no calculations)
        safe_cast(currency as string) as currency,
        safe_cast(total_refunded_amount as numeric) as total_refunded_amount,
        transactions,
        refund_line_items,

        -- Restock & inventory behavior
        safe_cast(restock as boolean) as restock,
        safe_cast(restock_type as string) as restock_type,

        -- Metadata
        safe_cast(note as string) as note,
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by refund_id 
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