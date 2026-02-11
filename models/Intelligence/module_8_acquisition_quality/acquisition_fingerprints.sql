{{
  config(
    materialized='table',
    unique_key=['acquisition_source_id', 'cohort_id']
  )
}}

with sources as (
  select source_id
  from {{ ref('acquisition_sources') }}
),

cohorts as (
  select distinct cohort_id
  from {{ ref('dim_cohort') }}
),

source_cohort_pairs as (
  select
    s.source_id as acquisition_source_id,
    c.cohort_id
  from sources s
  cross join cohorts c
),

with_identity as (
  select
    scp.acquisition_source_id,
    scp.cohort_id,
    cast(null as string) as fingerprint_identity_surface
  from source_cohort_pairs scp
),

with_motivation as (
  select
    wi.acquisition_source_id,
    wi.cohort_id,
    wi.fingerprint_identity_surface,
    mv.motivation_vector as fingerprint_motivation_profile
  from with_identity wi
  left join {{ ref('motivation_vectors') }} mv
    on wi.cohort_id = mv.cohort_id
),

with_price_meaning as (
  select
    wm.acquisition_source_id,
    wm.cohort_id,
    wm.fingerprint_identity_surface,
    wm.fingerprint_motivation_profile,
    pmv.price_meaning_vector as fingerprint_price_meaning_profile
  from with_motivation wm
  left join {{ ref('price_meaning_vectors') }} pmv
    on wm.cohort_id = pmv.cohort_id
),

with_friction as (
  select
    wpm.acquisition_source_id,
    wpm.cohort_id,
    wpm.fingerprint_identity_surface,
    wpm.fingerprint_motivation_profile,
    wpm.fingerprint_price_meaning_profile,
    pfp.psychological_friction_fingerprint as fingerprint_friction_profile
  from with_price_meaning wpm
  left join {{ ref('psychological_friction_profiles') }} pfp
    on wpm.cohort_id = pfp.cohort_id
),

with_phase as (
  select
    wf.acquisition_source_id,
    wf.cohort_id,
    wf.fingerprint_identity_surface,
    wf.fingerprint_motivation_profile,
    wf.fingerprint_price_meaning_profile,
    wf.fingerprint_friction_profile,
    cast(null as string) as fingerprint_phase_distribution
  from with_friction wf
)

select
  acquisition_source_id,
  cohort_id,
  fingerprint_identity_surface,
  fingerprint_motivation_profile,
  fingerprint_price_meaning_profile,
  fingerprint_friction_profile,
  fingerprint_phase_distribution,
  cast(null as numeric) as fingerprint_coherence,
  cast(null as numeric) as fingerprint_volatility
from with_phase
