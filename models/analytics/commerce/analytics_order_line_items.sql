{{
    config(
        materialized='table'
    )
}}

with line_items as (

    select * from {{ ref('stg_shopify_order_line_items') }}

),

orders as (

    select
        order_id,
        created_at as order_created_at,
        processed_at as order_processed_at

    from {{ ref('analytics_orders') }}

),

joined as (

    select
        line_items.*,
        orders.order_created_at,
        orders.order_processed_at

    from line_items
    left join orders using (order_id)

)

select
    -- Primary identifiers
    line_item_id,
    order_id,
    product_id,
    variant_id,
    sku,

    -- Product descriptors (as reported)
    product_title,
    variant_title,
    name,
    vendor,

    -- Quantity & fulfillment structure
    quantity,
    fulfillable_quantity,
    fulfillment_status,

    -- Monetary fields (as reported, no logic)
    price,
    total_discount,
    price_set_presentment_amount,
    price_set_shop_amount,

    -- Tax & shipping flags
    taxable,
    requires_shipping,

    -- Line-level metadata
    gift_card,
    properties,
    tax_lines,

    -- Timestamps from line item
    created_at,
    updated_at,

    -- Order-level timestamps (structural join, no grain change)
    order_created_at,
    order_processed_at

from joined