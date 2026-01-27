{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_orders') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as order_id,
        safe_cast(order_number as string) as order_number,
        safe_cast(checkout_id as string) as checkout_id,
        safe_cast(cart_token as string) as cart_token,

        -- Timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,
        safe_cast(processed_at as timestamp) as processed_at,
        safe_cast(cancelled_at as timestamp) as cancelled_at,
        safe_cast(closed_at as timestamp) as closed_at,

        -- Status fields (preserve Shopify semantics)
        safe_cast(financial_status as string) as financial_status,
        safe_cast(fulfillment_status as string) as fulfillment_status,
        safe_cast(order_status_url as string) as order_status_url,
        safe_cast(confirmed as boolean) as confirmed,
        safe_cast(test as boolean) as test,

        -- Monetary fields (as reported by Shopify, no conversions)
        safe_cast(currency as string) as currency,
        safe_cast(subtotal_price as numeric) as subtotal_price,
        safe_cast(total_price as numeric) as total_price,
        safe_cast(total_tax as numeric) as total_tax,
        safe_cast(total_discounts as numeric) as total_discounts,
        safe_cast(total_shipping_price as numeric) as total_shipping_price,
        safe_cast(total_refunded as numeric) as total_refunded,

        -- Customer & attribution references (no joins)
        safe_cast(customer_id as string) as customer_id,
        safe_cast(email as string) as email,
        safe_cast(source_name as string) as source_name,
        safe_cast(landing_site as string) as landing_site,
        safe_cast(referring_site as string) as referring_site,

        -- Order metadata
        safe_cast(tags as string) as tags,
        safe_cast(note as string) as note,
        safe_cast(shipping_address_country as string) as shipping_address_country,
        safe_cast(shipping_address_region as string) as shipping_address_region,
        safe_cast(billing_address_country as string) as billing_address_country,
        safe_cast(billing_address_region as string) as billing_address_region

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by order_id 
                order by updated_at desc
            ) as row_num
        from renamed
    )
    where row_num = 1

)

select
    -- Remove row_num helper column
    * except(row_num)

from deduped