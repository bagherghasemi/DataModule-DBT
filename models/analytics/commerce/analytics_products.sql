{{
    config(
        materialized='table'
    )
}}

with products as (

    select * from {{ ref('stg_shopify_products') }}

)

select
    -- Primary identifiers
    product_id,
    handle,

    -- Core metadata
    title,
    body_html,
    vendor,
    product_type,
    tags,

    -- Status & publishing
    status,
    published_at,
    published_scope,

    -- Lifecycle timestamps
    created_at,
    updated_at,

    -- Media & display
    image_src,
    image_alt_text,

    -- Shopify metadata
    has_only_default_variant,
    template_suffix,
    admin_graphql_api_id

from products