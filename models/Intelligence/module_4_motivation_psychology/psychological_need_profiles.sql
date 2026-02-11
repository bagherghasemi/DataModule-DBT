{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with cohorts_with_motivation as (
  select distinct cohort_id
  from {{ ref('motivation_vectors') }}
),

identity_by_cohort as (
  select
    cohort_id,
    sum(population_size) as total_population
  from {{ ref('identity_population_profiles') }}
  group by cohort_id
),

intent_by_cohort as (
  select cohort_id
  from {{ ref('intent_field_profiles') }}
  group by cohort_id
),

value_by_cohort as (
  select cohort_id
  from {{ ref('value_field_profiles') }}
  group by cohort_id
),

desire_value_by_cohort as (
  select cohort_id
  from {{ ref('desire_value_map') }}
  group by cohort_id
),

reciprocity_by_cohort as (
  select cohort_id
  from {{ ref('reciprocity_differential_vectors') }}
  group by cohort_id
),

yield_by_cohort as (
  select cohort_id
  from {{ ref('yield_stability_curves') }}
  group by cohort_id
),

base as (
  select
    mv.cohort_id
  from cohorts_with_motivation mv
  left join identity_by_cohort ibc
    on mv.cohort_id = ibc.cohort_id
  left join intent_by_cohort ifc
    on mv.cohort_id = ifc.cohort_id
  left join value_by_cohort vfc
    on mv.cohort_id = vfc.cohort_id
  left join desire_value_by_cohort dvbc
    on mv.cohort_id = dvbc.cohort_id
  left join reciprocity_by_cohort rbc
    on mv.cohort_id = rbc.cohort_id
  left join yield_by_cohort ybc
    on mv.cohort_id = ybc.cohort_id
)

select
  base.cohort_id,

  cast(null as numeric) as emotional_need_intensity,
  cast(null as numeric) as cognitive_need_intensity,
  cast(null as numeric) as identity_need_intensity,
  cast(null as numeric) as social_need_intensity,
  cast(null as numeric) as existential_need_intensity,

  cast(null as numeric) as need_coherence,
  cast(null as numeric) as unmet_need_pressure,
  cast(null as numeric) as psychological_fragility,

  struct(
    cast(null as numeric) as emotional_need_intensity,
    cast(null as numeric) as cognitive_need_intensity,
    cast(null as numeric) as identity_need_intensity,
    cast(null as numeric) as social_need_intensity,
    cast(null as numeric) as existential_need_intensity
  ) as psychological_need_profile

from base
