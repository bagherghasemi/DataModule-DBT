{{
    config(
        materialized='table'
    )
}}

with refunds as (

    select * from {{ ref('stg_shopify_refunds') }}

),

orders as (

    select
        order_id,
        created_at as order_created_at,
        processed_at as order_processed_at,
        financial_status,
        fulfillment_status

    from {{ ref('analytics_orders') }}

),

joined as (

    select
        refunds.*,
        orders.order_created_at,
        orders.order_processed_at,
        orders.financial_status,
        orders.fulfillment_status

    from refunds
    left join orders using (order_id)

)

select
    -- Primary identifiers
    refund_id,
    order_id,
    user_id,

    -- Timestamps
    created_at,
    processed_at,

    -- Monetary fields (as reported, no logic)
    currency,
    total_refunded_amount,
    refund_line_items,
    transactions,

    -- Inventory / restock metadata
    restock,
    restock_type,

    -- Freeform metadata
    note,
    admin_graphql_api_id,

    -- Order context (optional structural join)
    order_created_at,
    order_processed_at,
    financial_status,
    fulfillment_status

from joined