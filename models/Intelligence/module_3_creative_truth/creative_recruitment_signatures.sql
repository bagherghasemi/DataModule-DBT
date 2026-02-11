{{
  config(
    materialized='table',
    unique_key=['creative_id', 'cohort_id']
  )
}}

with creatives as (
  select distinct creative_id
  from {{ ref('creative_psychological_profiles') }}
  where creative_id is not null
),

cohorts_from_profiles as (
  select distinct ipp.cohort_id
  from {{ ref('identity_population_profiles') }} ipp
  inner join {{ ref('dim_cohort') }} dc
    on ipp.cohort_id = dc.cohort_id
),

meta_delivery_by_creative as (
  select
    aa.creative_id,
    sum(apd.impressions) as total_impressions,
    sum(apd.reach) as total_reach
  from {{ ref('analytics_ads') }} aa
  inner join {{ ref('analytics_ad_performance_daily') }} apd
    on aa.ad_id = apd.ad_id
  where aa.creative_id is not null
  group by aa.creative_id
),

creative_cohort_space as (
  select
    c.creative_id,
    ch.cohort_id
  from creatives c
  cross join cohorts_from_profiles ch
),

enum_guard as (
  select 1 as g from {{ ref('enum_population_stage') }} limit 1
)

select
  ccs.creative_id,
  ccs.cohort_id,

  cast(null as string) as dominant_identity_surface_region,
  cast(null as string) as dominant_value_shape_label,

  cast(null as numeric) as recruitment_probability,
  cast(null as numeric) as recruitment_strength,
  cast(null as numeric) as recruitment_volatility,

  struct(
    cast(null as numeric) as recruitment_probability,
    cast(null as numeric) as recruitment_strength,
    cast(null as numeric) as recruitment_volatility
  ) as human_recruitment_signature

from creative_cohort_space ccs
left join meta_delivery_by_creative mdc
  on ccs.creative_id = mdc.creative_id
cross join enum_guard eg
