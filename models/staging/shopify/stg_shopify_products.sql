{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_products') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as product_id,
        safe_cast(handle as string) as handle,

        -- Core metadata
        safe_cast(title as string) as title,
        safe_cast(body_html as string) as body_html,
        safe_cast(vendor as string) as vendor,
        safe_cast(product_type as string) as product_type,
        safe_cast(tags as string) as tags,

        -- Status & publishing
        safe_cast(status as string) as status,
        safe_cast(published_at as timestamp) as published_at,
        safe_cast(published_scope as string) as published_scope,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,

        -- Media & display
        safe_cast(image_src as string) as image_src,
        safe_cast(image_alt_text as string) as image_alt_text,

        -- Shopify-specific flags
        safe_cast(has_only_default_variant as boolean) as has_only_default_variant,
        safe_cast(template_suffix as string) as template_suffix,

        -- Miscellaneous metadata
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by product_id 
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