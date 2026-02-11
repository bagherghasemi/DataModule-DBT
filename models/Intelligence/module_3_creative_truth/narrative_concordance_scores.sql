{{
  config(
    materialized='table',
    unique_key='creative_id'
  )
}}

with creatives_with_profiles as (
  select
    creative_id,
    promise_volatility
  from {{ ref('creative_psychological_profiles') }}
  where creative_id is not null
),

recruitment_by_creative as (
  select
    creative_id,
    count(distinct cohort_id) as cohort_count
  from {{ ref('creative_recruitment_signatures') }}
  where creative_id is not null
  group by creative_id
),

identity_baseline as (
  select
    cohort_id,
    population_stage,
    population_size,
    normalized_presence_weight
  from {{ ref('identity_population_profiles') }}
),

cohorts_for_creatives as (
  select
    crs.creative_id,
    crs.cohort_id
  from {{ ref('creative_recruitment_signatures') }} crs
  inner join {{ ref('dim_cohort') }} dc
    on crs.cohort_id = dc.cohort_id
),

creative_cohort_baseline as (
  select
    cfc.creative_id,
    ib.cohort_id,
    ib.population_stage,
    ib.population_size,
    ib.normalized_presence_weight
  from cohorts_for_creatives cfc
  left join identity_baseline ib
    on cfc.cohort_id = ib.cohort_id
),

aggregated_baseline_by_creative as (
  select
    creative_id,
    count(distinct cohort_id) as cohorts_with_baseline,
    sum(coalesce(population_size, 0)) as total_baseline_population,
    sum(coalesce(normalized_presence_weight, 0)) as total_normalized_weight
  from creative_cohort_baseline
  group by creative_id
)

select
  cwp.creative_id,

  cast(null as string) as dominant_misalignment_axis,

  least(1.0, greatest(0.0, 1.0 - coalesce(cwp.promise_volatility, 0))) as narrative_behavior_concordance,

  cast(null as float64) as expectation_violation_rate,

  coalesce(cwp.promise_volatility, 0) as lived_experience_divergence,

  least(1.0, greatest(0.0, 1.0 - coalesce(cwp.promise_volatility, 0))) as narrative_concordance_score

from creatives_with_profiles cwp
left join recruitment_by_creative rbc
  on cwp.creative_id = rbc.creative_id
left join aggregated_baseline_by_creative abbc
  on cwp.creative_id = abbc.creative_id
