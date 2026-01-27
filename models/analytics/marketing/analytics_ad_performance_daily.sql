{{
    config(
        materialized='table',
        partition_by={
            'field': 'date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['ad_account_id', 'campaign_id', 'ad_set_id', 'ad_id']
    )
}}

with ad_performance as (

    select * from {{ ref('stg_meta_ad_performance_daily') }}

)

select
    -- Identifiers
    ad_id,
    ad_set_id,
    campaign_id,
    ad_account_id,

    -- Grain-defining date
    date,

    -- Delivery metrics (as reported)
    impressions,
    reach,
    frequency,

    -- Engagement metrics (as reported)
    clicks,
    unique_clicks,
    inline_link_clicks,
    outbound_clicks,
    landing_page_views,
    video_views,
    thruplays,
    post_engagements,

    -- Conversion metrics (as reported, preserve raw structure)
    conversions,
    purchase,
    add_to_cart,
    initiate_checkout,
    lead,
    complete_registration,
    custom_conversions,

    -- Financial metrics (as reported, not KPIs)
    spend,
    currency,
    cost_per_action_type,
    cost_per_conversion,

    -- Attribution & optimization metadata
    attribution_setting,
    optimization_goal,
    buying_type,

    -- Account-level structural metadata
    account_currency,
    account_timezone_name,
    account_timezone_offset_hours_utc,

    -- Ingestion metadata
    _fivetran_synced

from ad_performance