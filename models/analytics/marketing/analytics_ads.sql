{{
    config(
        materialized='table'
    )
}}

with ads as (

    select * from {{ ref('stg_meta_ads') }}

),

ad_sets as (

    select
        ad_set_id,
        ad_set_name

    from {{ ref('analytics_ad_sets') }}

),

campaigns as (

    select
        campaign_id,
        campaign_name,
        objective

    from {{ ref('analytics_campaigns') }}

),

joined as (

    select
        ads.*,
        ad_sets.ad_set_name,
        campaigns.campaign_name,
        campaigns.objective

    from ads
    left join ad_sets using (ad_set_id)
    left join campaigns using (campaign_id)

)

select
    -- Primary identifiers
    ad_id,
    ad_set_id,
    campaign_id,
    ad_account_id,

    -- Core metadata
    ad_name,

    -- Status & delivery
    status,
    effective_status,
    configured_status,

    -- Creative & asset references (structural only)
    creative_id,
    creative_name,
    creative_type,
    preview_url,

    -- Lifecycle timestamps
    created_time,
    updated_time,
    start_time,
    end_time,

    -- Tracking & configuration (passthrough)
    tracking_specs,
    conversion_domain,
    bid_amount,
    bid_type,

    -- Miscellaneous metadata
    source_ad_id,
    admin_graphql_api_id,

    -- Structural context (optional joins)
    ad_set_name,
    campaign_name,
    objective

from joined