{{
    config(
        materialized='table'
    )
}}

with ad_accounts as (

    select * from {{ ref('stg_meta_ad_accounts') }}

)

select
    -- Primary identifiers
    ad_account_id,
    account_name,

    -- Status & state
    account_status,
    is_personal,
    is_prepay_account,

    -- Currency & timezone
    currency,
    timezone_name,
    timezone_offset_hours_utc,

    -- Structural financial fields (not performance)
    balance,
    spend_cap,

    -- Lifecycle timestamps
    created_time,
    updated_time,

    -- Business context
    business_id,
    business_name,

    -- Miscellaneous metadata
    owner,
    admin_graphql_api_id

from ad_accounts