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
  select distinct cohort_id
  from {{ ref('identity_population_profiles') }}
),

friction_context as (
  select cohort_id
  from {{ ref('psychological_friction_profiles') }}
),

motivation_context as (
  select cohort_id
  from {{ ref('motivation_vectors') }}
),

price_meaning_context as (
  select cohort_id
  from {{ ref('price_meaning_vectors') }}
),

risk_context as (
  select cohort_id
  from {{ ref('risk_encoding_signatures') }}
),

base as (
  select ec.cohort_id
  from eligible_cohorts ec
  inner join identity_context ic on ec.cohort_id = ic.cohort_id
  left join friction_context fc on ec.cohort_id = fc.cohort_id
  left join motivation_context mc on ec.cohort_id = mc.cohort_id
  left join price_meaning_context pmc on ec.cohort_id = pmc.cohort_id
  left join risk_context rc on ec.cohort_id = rc.cohort_id
)

select
  base.cohort_id,

  cast(null as float64) as cognitive_load_complexity,
  cast(null as float64) as cognitive_load_choice_overload,
  cast(null as float64) as cognitive_load_uncertainty_processing,
  cast(null as float64) as cognitive_load_novelty_processing,

  cast(null as float64) as cognitive_load_tolerance,
  cast(null as float64) as cognitive_fragility

from base
