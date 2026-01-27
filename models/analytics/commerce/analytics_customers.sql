{{
    config(
        materialized='table'
    )
}}

with customers as (

    select * from {{ ref('stg_shopify_customers') }}

),

first_order_dates as (

    select
        customer_id,
        min(created_at) as first_order_at

    from {{ ref('analytics_orders') }}
    where customer_id is not null
    group by 1

),

joined as (

    select
        customers.*,
        first_order_dates.first_order_at

    from customers
    left join first_order_dates using (customer_id)

)

select
    -- Primary identifiers
    customer_id,
    email,
    phone,

    -- Name & profile
    first_name,
    last_name,
    display_name,
    locale,

    -- Lifecycle timestamps (Shopify-provided)
    created_at,
    updated_at,
    last_order_at,

    -- Structurally-derivable timestamps
    first_order_at,

    -- Customer status & flags
    customer_state,
    verified_email,
    accepts_marketing,
    tax_exempt,

    -- Default address (structural only)
    default_address_id,
    default_address_country,
    default_address_region,
    default_address_city,
    default_address_postal_code,

    -- Freeform metadata
    tags,
    note,
    source_name

from joined