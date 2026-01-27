{{
    config(
        materialized='table'
    )
}}

with orders as (

    select * from {{ ref('stg_shopify_orders') }}

)

select
    -- Primary identifiers
    order_id,
    order_number,
    checkout_id,
    cart_token,

    -- Customer reference (structural only, no joins)
    customer_id,
    email,

    -- Order lifecycle timestamps
    created_at,
    updated_at,
    processed_at,
    cancelled_at,
    closed_at,

    -- Status fields (as Shopify reports)
    financial_status,
    fulfillment_status,
    confirmed,
    test,
    order_status_url,

    -- Monetary fields (as reported, no logic)
    currency,
    subtotal_price,
    total_price,
    total_tax,
    total_discounts,
    total_shipping_price,
    total_refunded,

    -- Attribution & source metadata
    source_name,
    landing_site,
    referring_site,

    -- Geography (structural only)
    shipping_address_country,
    shipping_address_region,
    billing_address_country,
    billing_address_region,

    -- Freeform metadata
    tags,
    note

from orders