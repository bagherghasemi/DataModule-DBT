{{
    config(
        materialized='table'
    )
}}

with ad_sets as (

    select * from {{ ref('stg_meta_ad_sets') }}

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
        ad_sets.*,
        campaigns.campaign_name,
        campaigns.objective

    from ad_sets
    left join campaigns using (campaign_id)

)

select
    -- Primary identifiers
    ad_set_id,
    campaign_id,
    ad_account_id,

    -- Core metadata
    ad_set_name,

    -- Status & delivery
    status,
    effective_status,
    configured_status,

    -- Budget & bidding structure (as reported, not KPIs)
    daily_budget,
    lifetime_budget,
    budget_remaining,
    bid_amount,
    billing_event,
    optimization_goal,

    -- Targeting & configuration (passthrough)
    targeting,
    promoted_object,

    -- Lifecycle timestamps
    created_time,
    updated_time,
    start_time,
    end_time,

    -- Miscellaneous metadata
    source_ad_set_id,
    admin_graphql_api_id,

    -- Campaign context (optional structural join)
    campaign_name,
    objective

from joined