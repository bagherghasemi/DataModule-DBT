{{
    config(
        materialized='incremental',
        unique_key='fulfillment_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_fulfillments') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    -- Use created_at if updated_at is not available
    where coalesce(updated_at, created_at) > (select max(coalesce(updated_at, created_at)) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as fulfillment_id,
        safe_cast(order_id as string) as order_id,
        safe_cast(location_id as string) as location_id,

        -- Fulfillment status
        safe_cast(status as string) as status,
        safe_cast(shipment_status as string) as shipment_status,

        -- Timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,

        -- Tracking information (as reported by Shopify)
        safe_cast(tracking_company as string) as tracking_company,
        safe_cast(tracking_number as string) as tracking_number,
        tracking_numbers,
        safe_cast(tracking_url as string) as tracking_url,
        tracking_urls,

        -- Fulfillment metadata
        safe_cast(service as string) as service,
        safe_cast(name as string) as name,
        receipt,
        line_items,

        -- Shopify metadata
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by fulfillment_id 
                order by coalesce(updated_at, created_at) desc
            ) as row_num
        from renamed
    )
    where row_num = 1

)

select
    -- Remove row_num helper column
    * except(row_num)

from deduped