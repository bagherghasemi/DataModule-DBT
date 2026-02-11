{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with trust_cohorts as (
  select distinct cohort_id from {{ ref('trust_formation_curves') }}
),

motivation_cohorts as (
  select distinct cohort_id from {{ ref('motivation_vectors') }}
),

emotional_cohorts as (
  select distinct cohort_id from {{ ref('emotional_payoff_curves') }}
),

identity_cohorts as (
  select distinct cohort_id from {{ ref('identity_transition_matrix') }}
),

cohorts_with_context as (
  select tc.cohort_id
  from trust_cohorts tc
  inner join motivation_cohorts mc on tc.cohort_id = mc.cohort_id
  inner join emotional_cohorts ec on tc.cohort_id = ec.cohort_id
  inner join identity_cohorts ic on tc.cohort_id = ic.cohort_id
)

select
  cohort_id,
  cast(null as float64) as attachment_velocity,
  cast(null as float64) as attachment_acceleration
from cohorts_with_context
