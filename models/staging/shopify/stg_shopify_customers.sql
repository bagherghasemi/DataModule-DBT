{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        on_schema_change='fail',
        partition_by={
            'field': 'created_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

with source as (

    select * from {{ source('shopify', 'shopify_customers') }}
    
    {% if is_incremental() %}
    -- Only process records that have been updated since the last run
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Identifiers
        safe_cast(id as string) as customer_id,
        safe_cast(email as string) as email,
        safe_cast(phone as string) as phone,

        -- Customer name & profile
        safe_cast(first_name as string) as first_name,
        safe_cast(last_name as string) as last_name,
        safe_cast(display_name as string) as display_name,
        safe_cast(locale as string) as locale,

        -- Lifecycle timestamps (UTC)
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,
        safe_cast(last_order_at as timestamp) as last_order_at,

        -- Customer status & flags
        safe_cast(state as string) as customer_state,
        safe_cast(verified_email as boolean) as verified_email,
        safe_cast(accepts_marketing as boolean) as accepts_marketing,
        safe_cast(tax_exempt as boolean) as tax_exempt,

        -- Default address (as stored on customer, no normalization)
        safe_cast(default_address_id as string) as default_address_id,
        safe_cast(default_address_country as string) as default_address_country,
        safe_cast(default_address_region as string) as default_address_region,
        safe_cast(default_address_city as string) as default_address_city,
        safe_cast(default_address_postal_code as string) as default_address_postal_code,

        -- Marketing & metadata
        safe_cast(tags as string) as tags,
        safe_cast(note as string) as note,
        safe_cast(source_name as string) as source_name

    from source

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by customer_id 
                order by updated_at desc
            ) as row_num
        from renamed
    )
    where row_num = 1

)

select
    -- Remove row_num helper column
    * except(row_num)

from deduped