{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with price_meaning_base as (
  select
    cohort_id,
    price_meaning_smart_choice_intensity,
    price_meaning_investing_in_self_intensity,
    price_meaning_safety_intensity,
    price_meaning_treat_intensity,
    price_meaning_identity_intensity,
    price_meaning_coherence,
    price_meaning_volatility
  from {{ ref('price_meaning_vectors') }}
),

motivation_context as (
  select
    cohort_id,
    motivation_status_intensity,
    motivation_safety_intensity,
    motivation_belonging_intensity,
    motivation_coherence,
    motivation_conflict_index
  from {{ ref('motivation_vectors') }}
),

identity_baseline as (
  select
    cohort_id,
    sum(population_size) as total_population,
    avg(stability_orientation_score) as avg_stability_orientation,
    avg(belonging_orientation_score) as avg_belonging_orientation,
    avg(aspirational_score) as avg_aspirational_score,
    avg(behavioral_coherence) as avg_behavioral_coherence
  from {{ ref('identity_population_profiles') }}
  group by cohort_id
)

select
  pmb.cohort_id,

  cast(null as numeric) as rational_value_weight,
  cast(null as numeric) as emotional_value_weight,
  cast(null as numeric) as identity_value_weight,
  cast(null as numeric) as social_value_weight,
  cast(null as numeric) as security_value_weight,

  cast(null as numeric) as interpretation_consistency,
  cast(null as numeric) as interpretive_conflict_index

from price_meaning_base pmb
left join motivation_context mc
  on pmb.cohort_id = mc.cohort_id
left join identity_baseline ib
  on pmb.cohort_id = ib.cohort_id
