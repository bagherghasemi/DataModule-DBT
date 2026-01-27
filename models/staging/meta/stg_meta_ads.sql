{{
    config(
        materialized='incremental',
        unique_key='ad_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_time',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('meta', 'meta_ads') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_time > (select max(updated_time) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as ad_id,
        safe_cast(adset_id as string) as ad_set_id,
        safe_cast(campaign_id as string) as campaign_id,
        safe_cast(account_id as string) as ad_account_id,

        -- Core metadata
        safe_cast(name as string) as ad_name,

        -- Status & delivery
        safe_cast(status as string) as status,
        safe_cast(effective_status as string) as effective_status,
        safe_cast(configured_status as string) as configured_status,

        -- Creative & asset references (structural only)
        safe_cast(creative_id as string) as creative_id,
        safe_cast(creative_name as string) as creative_name,
        safe_cast(creative_type as string) as creative_type,
        safe_cast(preview_url as string) as preview_url,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_time as timestamp) as created_time,
        safe_cast(updated_time as timestamp) as updated_time,
        safe_cast(start_time as timestamp) as start_time,
        safe_cast(end_time as timestamp) as end_time,

        -- Tracking & configuration (passthrough)
        tracking_specs,
        safe_cast(conversion_domain as string) as conversion_domain,
        safe_cast(bid_amount as numeric) as bid_amount,
        safe_cast(bid_type as string) as bid_type,

        -- Miscellaneous metadata
        safe_cast(source_ad_id as string) as source_ad_id,
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by ad_id 
                order by updated_time desc
            ) as row_num
        from renamed
    )
    where row_num = 1

)

select
    -- Remove row_num helper column
    * except(row_num)

from deduped