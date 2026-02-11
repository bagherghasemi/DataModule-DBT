{{
  config(
    materialized='table',
    unique_key='creative_id'
  )
}}

with creative_universe as (
  select distinct
    creative_id
  from {{ ref('analytics_ads') }}
  where creative_id is not null
),

creative_performance as (
  select
    aa.creative_id,
    apd.date,
    apd.impressions,
    apd.reach,
    apd.clicks,
    apd.video_views,
    apd.post_engagements
  from {{ ref('analytics_ads') }} aa
  inner join {{ ref('analytics_ad_performance_daily') }} apd
    on aa.ad_id = apd.ad_id
  where aa.creative_id is not null
),

creative_delivery_by_date as (
  select
    creative_id,
    date,
    sum(impressions) as impressions,
    sum(reach) as reach,
    sum(clicks) as clicks,
    sum(video_views) as video_views,
    sum(post_engagements) as post_engagements
  from creative_performance
  group by creative_id, date
),

delivery_volatility as (
  select
    creative_id,
    case
      when avg(impressions) is null or avg(impressions) = 0 then 0
      else greatest(0, coalesce(stddev(impressions), 0) / avg(impressions))
    end as promise_volatility
  from creative_delivery_by_date
  group by creative_id
)

select
  cu.creative_id,

  cast(null as numeric) as promise_status_intensity,
  cast(null as numeric) as promise_safety_intensity,
  cast(null as numeric) as promise_control_intensity,
  cast(null as numeric) as promise_belonging_intensity,
  cast(null as numeric) as promise_transformation_intensity,
  cast(null as numeric) as promise_relief_intensity,
  cast(null as numeric) as promise_novelty_intensity,
  cast(null as numeric) as promise_mastery_intensity,

  cast(null as numeric) as promise_coherence,
  coalesce(dv.promise_volatility, 0) as promise_volatility,

  struct(
    cast(null as numeric) as promise_status_intensity,
    cast(null as numeric) as promise_safety_intensity,
    cast(null as numeric) as promise_control_intensity,
    cast(null as numeric) as promise_belonging_intensity,
    cast(null as numeric) as promise_transformation_intensity,
    cast(null as numeric) as promise_relief_intensity,
    cast(null as numeric) as promise_novelty_intensity,
    cast(null as numeric) as promise_mastery_intensity
  ) as psychological_promise_vector

from creative_universe cu
left join delivery_volatility dv
  on cu.creative_id = dv.creative_id
