{{
    config(
        materialized='incremental',
        unique_key='ad_account_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_time',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('meta', 'meta_ad_accounts') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_time > (select max(updated_time) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as ad_account_id,
        safe_cast(name as string) as account_name,

        -- Status & state
        safe_cast(account_status as string) as account_status,
        safe_cast(is_personal as boolean) as is_personal,
        safe_cast(is_prepay_account as boolean) as is_prepay_account,

        -- Currency & timezone
        safe_cast(currency as string) as currency,
        safe_cast(timezone_name as string) as timezone_name,
        safe_cast(timezone_offset_hours_utc as numeric) as timezone_offset_hours_utc,

        -- Financial structure (as reported, not metrics)
        safe_cast(amount_spent as numeric) as amount_spent,
        safe_cast(balance as numeric) as balance,
        safe_cast(spend_cap as numeric) as spend_cap,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_time as timestamp) as created_time,
        safe_cast(updated_time as timestamp) as updated_time,

        -- Business metadata
        safe_cast(business_id as string) as business_id,
        safe_cast(business_name as string) as business_name,

        -- Miscellaneous metadata
        safe_cast(owner as string) as owner,
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by ad_account_id 
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