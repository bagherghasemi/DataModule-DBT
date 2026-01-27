{{
    config(
        materialized='incremental',
        unique_key='ad_set_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_time',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('meta', 'meta_ad_sets') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_time > (select max(updated_time) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as ad_set_id,
        safe_cast(campaign_id as string) as campaign_id,
        safe_cast(account_id as string) as ad_account_id,

        -- Core metadata
        safe_cast(name as string) as ad_set_name,

        -- Status & delivery
        safe_cast(status as string) as status,
        safe_cast(effective_status as string) as effective_status,
        safe_cast(configured_status as string) as configured_status,

        -- Budget structure (as reported, not performance)
        safe_cast(daily_budget as numeric) as daily_budget,
        safe_cast(lifetime_budget as numeric) as lifetime_budget,
        safe_cast(budget_remaining as numeric) as budget_remaining,
        safe_cast(bid_amount as numeric) as bid_amount,
        safe_cast(billing_event as string) as billing_event,
        safe_cast(optimization_goal as string) as optimization_goal,

        -- Targeting & configuration (passthrough)
        targeting,
        promoted_object,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_time as timestamp) as created_time,
        safe_cast(updated_time as timestamp) as updated_time,
        safe_cast(start_time as timestamp) as start_time,
        safe_cast(end_time as timestamp) as end_time,

        -- Miscellaneous metadata
        safe_cast(source_ad_set_id as string) as source_ad_set_id,
        safe_cast(admin_graphql_api_id as string) as admin_graphql_api_id

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by ad_set_id 
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