{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with cohorts as (
  select distinct cohort_id
  from {{ ref('dim_cohort') }}
),

regret_signals as (
  select
    cohort_id,
    regret_probability,
    regret_latency,
    regret_persistence
  from {{ ref('regret_formation_indices') }}
),

trust_repair as (
  select
    cohort_id,
    trust_repairability_score
  from {{ ref('trust_decay_signatures') }}
),

motivation as (
  select cohort_id
  from {{ ref('motivation_vectors') }}
),

identity_anchor as (
  select
    cohort_id,
    identity_anchoring_score
  from {{ ref('price_meaning_vectors') }}
),

base as (
  select c.cohort_id
  from cohorts c
  left join regret_signals r on c.cohort_id = r.cohort_id
  left join trust_repair t on c.cohort_id = t.cohort_id
  left join motivation m on c.cohort_id = m.cohort_id
  left join identity_anchor i on c.cohort_id = i.cohort_id
)

select
  cohort_id,
  cast(null as float64) as return_elasticity_score
from base
