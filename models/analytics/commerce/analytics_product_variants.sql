{{
    config(
        materialized='table'
    )
}}

with variants as (

    select * from {{ ref('stg_shopify_product_variants') }}

),

products as (

    select
        product_id,
        title as product_title,
        vendor as product_vendor,
        product_type,
        status as product_status

    from {{ ref('analytics_products') }}

),

joined as (

    select
        variants.*,
        products.product_title,
        products.product_vendor,
        products.product_type,
        products.product_status

    from variants
    left join products using (product_id)

)

select
    -- Primary identifiers
    variant_id,
    product_id,
    sku,
    barcode,

    -- Descriptors
    title,
    option_1,
    option_2,
    option_3,

    -- Pricing (as reported, no math)
    price,
    compare_at_price,
    presentment_prices,

    -- Inventory structure (not performance)
    inventory_item_id,
    inventory_management,
    inventory_policy,
    inventory_quantity,
    requires_shipping,
    taxable,
    weight,
    weight_unit,
    available_for_sale,
    position,

    -- Lifecycle timestamps
    created_at,
    updated_at,

    -- Shopify metadata
    image_id,
    admin_graphql_api_id,

    -- Product context (optional structural join)
    product_title,
    product_vendor,
    product_type,
    product_status

from joined