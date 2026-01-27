{{
    config(
        materialized='table'
    )
}}

with campaigns as (

    select * from {{ ref('stg_meta_campaigns') }}

),

ad_accounts as (

    select
        ad_account_id,
        account_name,
        currency,
        timezone_name

    from {{ ref('analytics_ad_accounts') }}

),

joined as (

    select
        campaigns.*,
        ad_accounts.account_name,
        ad_accounts.currency,
        ad_accounts.timezone_name

    from campaigns
    left join ad_accounts using (ad_account_id)

)

select
    -- Primary identifiers
    campaign_id,
    ad_account_id,

    -- Core metadata
    campaign_name,
    objective,
    buying_type,
    special_ad_categories,

    -- Status & delivery
    status,
    effective_status,
    configured_status,

    -- Lifecycle timestamps
    created_time,
    updated_time,
    start_time,
    stop_time,

    -- Budget structure (as reported, not KPIs)
    daily_budget,
    lifetime_budget,
    budget_remaining,

    -- Miscellaneous metadata
    source_campaign_id,
    promoted_object,
    admin_graphql_api_id,

    -- Account context (optional structural join)
    account_name,
    currency,
    timezone_name

from joined