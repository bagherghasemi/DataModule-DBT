{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

/*
  Model: dim_cohort
  Layer: Shared dimension (foundational ontology)
  Grain: One row per canonically defined cohort
  Purpose: Deterministic, recomputable cohort ontology
*/

with meta_acquisition_signals as (
  select distinct
    ac.campaign_id,
    aas.ad_set_id,
    aa.ad_id,
    aa.creative_id,
    aas.audience_type,
    aapd.platform,
    aapd.date_day as first_touch_date,
    ac.campaign_start_date
  from {{ ref('analytics_ad_performance_daily') }} aapd
  inner join {{ ref('analytics_ads') }} aa 
    on aapd.ad_id = aa.ad_id
  inner join {{ ref('analytics_ad_sets') }} aas 
    on aa.ad_set_id = aas.ad_set_id
  inner join {{ ref('analytics_campaigns') }} ac 
    on aas.campaign_id = ac.campaign_id
  where aapd.impressions > 0 or aapd.clicks > 0
),

shopify_behavioral_signals as (
  select distinct
    ao.customer_id,
    ao.order_id,
    ao.created_at as order_date,
    ao.total_price as order_value,
    ap.product_category as product_category,
    ao.discount_code,
    ao.country,
    ao.region,
    ao.city,
    ar.refund_id,
    row_number() over (
      partition by ao.customer_id 
      order by ao.created_at
    ) as purchase_sequence
  from {{ ref('analytics_orders') }} ao
  left join {{ ref('analytics_products') }} ap 
    on ao.product_id = ap.product_id
  left join {{ ref('analytics_refunds') }} ar 
    on ao.order_id = ar.order_id
  inner join {{ ref('analytics_customers') }} ac 
    on ao.customer_id = ac.customer_id
),

first_purchase_attributes as (
  select
    customer_id,
    product_category as first_product_category,
    order_value as first_order_value,
    order_date as first_purchase_date,
    discount_code as first_discount_code,
    country,
    region,
    city,
    refund_id as first_order_refund_id
  from shopify_behavioral_signals
  where purchase_sequence = 1
),

second_purchase_timing as (
  select
    sbs.customer_id,
    sbs.order_date as second_purchase_date,
    date_diff(sbs.order_date, fpa.first_purchase_date, day) as days_to_second_purchase
  from shopify_behavioral_signals sbs
  inner join first_purchase_attributes fpa 
    on sbs.customer_id = fpa.customer_id
  where sbs.purchase_sequence = 2
),

cohort_defining_attributes as (
  select
    -- Acquisition axes (Meta-native)
    mas.campaign_id,
    mas.ad_set_id,
    mas.ad_id,
    mas.creative_id,
    mas.audience_type,
    mas.platform,
    
    -- Temporal axes - first touch
    date_trunc(mas.first_touch_date, week) as first_touch_week,
    
    -- Behavioral axes (Shopify-native)
    fpa.first_product_category,
    fpa.country,
    fpa.region,
    fpa.city,
    
    -- Raw values for bucketing
    fpa.first_order_value,
    fpa.first_discount_code,
    fpa.first_order_refund_id,
    
    -- Temporal axes - first purchase
    date_trunc(fpa.first_purchase_date, month) as first_purchase_month,
    
    -- Raw values for repeat behavior
    spt.days_to_second_purchase,
    
    -- Raw values for campaign launch window
    mas.first_touch_date,
    mas.campaign_start_date,
    
    -- Track earliest occurrence for cohort birth
    min(coalesce(mas.first_touch_date, fpa.first_purchase_date)) as earliest_occurrence
    
  from meta_acquisition_signals mas
  full outer join first_purchase_attributes fpa
    on 1=1
  left join second_purchase_timing spt
    on fpa.customer_id = spt.customer_id
  
  group by
    mas.campaign_id,
    mas.ad_set_id,
    mas.ad_id,
    mas.creative_id,
    mas.audience_type,
    mas.platform,
    date_trunc(mas.first_touch_date, week),
    fpa.first_product_category,
    fpa.country,
    fpa.region,
    fpa.city,
    fpa.first_order_value,
    fpa.first_discount_code,
    fpa.first_order_refund_id,
    date_trunc(fpa.first_purchase_date, month),
    spt.days_to_second_purchase,
    mas.first_touch_date,
    mas.campaign_start_date
),

cohort_with_buckets as (
  select
    campaign_id,
    ad_set_id,
    ad_id,
    creative_id,
    audience_type,
    platform,
    first_touch_week,
    first_product_category,
    country,
    region,
    city,
    
    -- AOV bucket
    case
      when first_order_value < 50 then 'low'
      when first_order_value < 150 then 'medium'
      when first_order_value < 300 then 'high'
      else 'very_high'
    end as aov_bucket,
    
    -- Discount usage pattern
    case
      when first_discount_code is not null then 'discount_user'
      else 'full_price'
    end as discount_usage_pattern,
    
    -- Refund behavior
    case
      when first_order_refund_id is not null then 'refunded'
      else 'no_refund'
    end as refund_behavior,
    
    first_purchase_month,
    
    -- Repeat behavior / time to second purchase bucket
    case
      when days_to_second_purchase is null then 'no_repeat'
      when days_to_second_purchase <= 7 then 'repeat_within_week'
      when days_to_second_purchase <= 30 then 'repeat_within_month'
      when days_to_second_purchase <= 90 then 'repeat_within_quarter'
      else 'repeat_after_quarter'
    end as repeat_behavior,
    
    -- Campaign launch window (temporal context)
    case
      when date_diff(first_touch_date, campaign_start_date, day) <= 7 then 'launch_week'
      when date_diff(first_touch_date, campaign_start_date, day) <= 30 then 'launch_month'
      else 'mature_campaign'
    end as campaign_launch_window,
    
    earliest_occurrence
    
  from cohort_defining_attributes
),

cohort_with_deterministic_id as (
  select
    -- Generate deterministic cohort_id from ordered cohort-defining attributes
    {{ dbt_utils.generate_surrogate_key([
      'campaign_id',
      'ad_set_id', 
      'ad_id',
      'creative_id',
      'audience_type',
      'platform',
      'first_touch_week',
      'first_product_category',
      'country',
      'region',
      'city',
      'aov_bucket',
      'discount_usage_pattern',
      'refund_behavior',
      'first_purchase_month',
      'repeat_behavior',
      'campaign_launch_window'
    ]) }} as cohort_id,
    
    -- All cohort-defining attributes
    campaign_id,
    ad_set_id,
    ad_id,
    creative_id,
    audience_type,
    platform,
    first_touch_week,
    first_product_category,
    country,
    region,
    city,
    aov_bucket,
    discount_usage_pattern,
    refund_behavior,
    first_purchase_month,
    repeat_behavior,
    campaign_launch_window,
    
    -- Cohort birth timestamp
    earliest_occurrence as cohort_birth_timestamp
    
  from cohort_with_buckets
),

current_activity_check as (
  select
    cwdi.cohort_id,
    case
      when exists (
        select 1 
        from meta_acquisition_signals mas2
        where mas2.campaign_id = cwdi.campaign_id
          and mas2.ad_set_id = cwdi.ad_set_id
          and mas2.first_touch_date >= date_sub(current_date(), interval 90 day)
      )
      or exists (
        select 1
        from shopify_behavioral_signals sbs2
        where sbs2.country = cwdi.country
          and sbs2.order_date >= date_sub(current_date(), interval 90 day)
      )
      then true
      else false
    end as is_active_cohort
  from cohort_with_deterministic_id cwdi
)

-- Final output: one row per unique cohort definition
select
  cwdi.cohort_id,
  
  -- Acquisition axes (Meta-native)
  cwdi.campaign_id,
  cwdi.ad_set_id,
  cwdi.ad_id,
  cwdi.creative_id,
  cwdi.audience_type,
  cwdi.platform,
  
  -- Behavioral axes (Shopify-native)
  cwdi.first_product_category,
  cwdi.aov_bucket,
  cwdi.discount_usage_pattern,
  cwdi.refund_behavior,
  cwdi.repeat_behavior,
  
  -- Temporal axes
  cwdi.first_purchase_month,
  cwdi.first_touch_week,
  cwdi.campaign_launch_window,
  
  -- Geographic axes
  cwdi.country,
  cwdi.region,
  cwdi.city,
  
  -- Temporal markers
  cwdi.cohort_birth_timestamp,
  
  -- Stability markers
  cac.is_active_cohort

from cohort_with_deterministic_id cwdi
inner join current_activity_check cac
  on cwdi.cohort_id = cac.cohort_id