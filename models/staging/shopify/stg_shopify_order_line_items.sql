{{
    config(
        materialized='incremental',
        unique_key='line_item_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_order_line_items') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as line_item_id,
        safe_cast(order_id as string) as order_id,
        safe_cast(product_id as string) as product_id,
        safe_cast(variant_id as string) as variant_id,
        safe_cast(sku as string) as sku,

        -- Product descriptors (as reported)
        safe_cast(product_title as string) as product_title,
        safe_cast(variant_title as string) as variant_title,
        safe_cast(vendor as string) as vendor,
        safe_cast(name as string) as name,

        -- Quantity & fulfillment
        safe_cast(quantity as int64) as quantity,
        safe_cast(fulfillable_quantity as int64) as fulfillable_quantity,
        safe_cast(fulfillment_status as string) as fulfillment_status,

        -- Monetary fields (as reported by Shopify, no calculations)
        safe_cast(price as numeric) as price,
        safe_cast(price_set_presentment_amount as numeric) as price_set_presentment_amount,
        safe_cast(price_set_shop_amount as numeric) as price_set_shop_amount,
        safe_cast(total_discount as numeric) as total_discount,
        discount_allocations,
        safe_cast(taxable as boolean) as taxable,

        -- Tax & shipping indicators
        safe_cast(requires_shipping as boolean) as requires_shipping,
        tax_lines,

        -- Timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,

        -- Miscellaneous metadata
        safe_cast(gift_card as boolean) as gift_card,
        properties,
        safe_cast(origin_location_id as string) as origin_location_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by line_item_id 
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