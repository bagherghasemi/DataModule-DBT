{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with eligible_cohorts as (
  select distinct cohort_id
  from {{ ref('dim_cohort') }}
),

identity_context as (
  select
    cohort_id,
    sum(population_size) as total_population
  from {{ ref('identity_population_profiles') }}
  group by cohort_id
),

motivation_context as (
  select cohort_id
  from {{ ref('motivation_vectors') }}
),

price_meaning_context as (
  select cohort_id
  from {{ ref('price_meaning_vectors') }}
),

base as (
  select ec.cohort_id
  from eligible_cohorts ec
  left join identity_context ic on ec.cohort_id = ic.cohort_id
  left join motivation_context mc on ec.cohort_id = mc.cohort_id
  left join price_meaning_context pmc on ec.cohort_id = pmc.cohort_id
)

select
  base.cohort_id,

  cast(null as numeric) as friction_trust_intensity,
  cast(null as numeric) as friction_risk_intensity,
  cast(null as numeric) as friction_cognitive_intensity,
  cast(null as numeric) as friction_identity_intensity,
  cast(null as numeric) as friction_control_intensity,
  cast(null as numeric) as friction_expectation_intensity,

  cast(null as numeric) as friction_coherence,
  cast(null as numeric) as friction_volatility,

  struct(
    cast(null as numeric) as friction_trust_intensity,
    cast(null as numeric) as friction_risk_intensity,
    cast(null as numeric) as friction_cognitive_intensity,
    cast(null as numeric) as friction_identity_intensity,
    cast(null as numeric) as friction_control_intensity,
    cast(null as numeric) as friction_expectation_intensity
  ) as psychological_friction_fingerprint,

  cast(null as numeric) as commitment_readiness_score

from base
