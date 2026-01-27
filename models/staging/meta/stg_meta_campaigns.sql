{{
    config(
        materialized='incremental',
        unique_key='campaign_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_time',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('meta', 'meta_campaigns') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_time > (select max(updated_time) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as campaign_id,
        safe_cast(account_id as string) as ad_account_id,

        -- Core metadata
        safe_cast(name as string) as campaign_name,
        safe_cast(objective as string) as objective,
        safe_cast(buying_type as string) as buying_type,
        special_ad_categories,

        -- Status & delivery
        safe_cast(status as string) as status,
        safe_cast(effective_status as string) as effective_status,
        safe_cast(configured_status as string) as configured_status,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_time as timestamp) as created_time,
        safe_cast(updated_time as timestamp) as updated_time,
        safe_cast(start_time as timestamp) as start_time,
        safe_cast(stop_time as timestamp) as stop_time,

        -- Budget structure (as reported, not performance)
        safe_cast(daily_budget as numeric) as daily_budget,
        safe_cast(lifetime_budget as numeric) as lifetime_budget,
        safe_cast(budget_remaining as numeric) as budget_remaining,

        -- Miscellaneous metadata
        safe_cast(source_campaign_id as string) as source_campaign_id,
        promoted_object,
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by campaign_id 
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