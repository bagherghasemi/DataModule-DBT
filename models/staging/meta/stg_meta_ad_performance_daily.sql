{{
    config(
        materialized='incremental',
        unique_key=['ad_id', 'date'],
        on_schema_change='fail',
        partition_by={
            'field': 'date',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('meta', 'meta_ad_performance_daily') }}
    
    {% if is_incremental() %}
    -- Allow late-arriving updates and backfills
    where date >= date_sub(current_date(), interval 60 day)
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(ad_id as string) as ad_id,
        safe_cast(adset_id as string) as ad_set_id,
        safe_cast(campaign_id as string) as campaign_id,
        safe_cast(account_id as string) as ad_account_id,

        -- Date (grain-defining)
        safe_cast(date as date) as date,

        -- Core delivery metrics (as reported)
        safe_cast(impressions as int64) as impressions,
        safe_cast(reach as int64) as reach,
        safe_cast(frequency as numeric) as frequency,

        -- Engagement metrics (as reported)
        safe_cast(clicks as int64) as clicks,
        safe_cast(unique_clicks as int64) as unique_clicks,
        safe_cast(inline_link_clicks as int64) as inline_link_clicks,
        safe_cast(outbound_clicks as int64) as outbound_clicks,
        safe_cast(landing_page_views as int64) as landing_page_views,
        safe_cast(video_views as int64) as video_views,
        safe_cast(thruplays as int64) as thruplays,
        safe_cast(post_engagements as int64) as post_engagements,

        -- Conversion metrics (as reported, may be nested)
        safe_cast(conversions as int64) as conversions,
        safe_cast(purchase as int64) as purchase,
        safe_cast(add_to_cart as int64) as add_to_cart,
        safe_cast(initiate_checkout as int64) as initiate_checkout,
        safe_cast(lead as int64) as lead,
        safe_cast(complete_registration as int64) as complete_registration,
        custom_conversions,

        -- Financial metrics (as reported, not KPIs)
        safe_cast(spend as numeric) as spend,
        safe_cast(currency as string) as currency,
        cost_per_action_type,
        cost_per_conversion,

        -- Attribution & metadata
        safe_cast(attribution_setting as string) as attribution_setting,
        safe_cast(optimization_goal as string) as optimization_goal,
        safe_cast(buying_type as string) as buying_type,

        -- Connector metadata
        safe_cast(account_currency as string) as account_currency,
        safe_cast(account_timezone_name as string) as account_timezone_name,
        safe_cast(account_timezone_offset_hours_utc as numeric) as account_timezone_offset_hours_utc,

        -- Ingestion metadata
        safe_cast(_fivetran_synced as timestamp) as _fivetran_synced

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by ad_id, date 
                order by _fivetran_synced desc
            ) as row_num
        from renamed
    )
    where row_num = 1

)

select
    -- Remove row_num helper column
    * except(row_num)

from deduped