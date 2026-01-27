{{
    config(
        materialized='incremental',
        unique_key='variant_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_product_variants') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as variant_id,
        safe_cast(product_id as string) as product_id,
        safe_cast(sku as string) as sku,
        safe_cast(barcode as string) as barcode,

        -- Descriptors
        safe_cast(title as string) as title,
        safe_cast(option_1 as string) as option_1,
        safe_cast(option_2 as string) as option_2,
        safe_cast(option_3 as string) as option_3,

        -- Pricing (as reported by Shopify, no calculations)
        safe_cast(price as numeric) as price,
        safe_cast(compare_at_price as numeric) as compare_at_price,
        presentment_prices,

        -- Inventory structure (not metrics, no interpretation)
        safe_cast(inventory_item_id as string) as inventory_item_id,
        safe_cast(inventory_management as string) as inventory_management,
        safe_cast(inventory_policy as string) as inventory_policy,
        safe_cast(inventory_quantity as int64) as inventory_quantity,
        safe_cast(requires_shipping as boolean) as requires_shipping,
        safe_cast(taxable as boolean) as taxable,
        safe_cast(weight as numeric) as weight,
        safe_cast(weight_unit as string) as weight_unit,

        -- Status & publishing
        safe_cast(available_for_sale as boolean) as available_for_sale,
        safe_cast(position as int64) as position,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,

        -- Shopify metadata
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id,
        safe_cast(image_id as string) as image_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by variant_id 
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