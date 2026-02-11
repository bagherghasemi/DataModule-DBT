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

transition_context as (
  select distinct cohort_id
  from {{ ref('identity_transition_matrix') }}
)

select
  c.cohort_id,
  cast(null as string) as trust_decay_signature,
  cast(null as float64) as trust_fragility_index,
  cast(null as float64) as trust_repairability_score,
  cast(null as float64) as collapse_speed
from cohorts c
left join transition_context tc
  on c.cohort_id = tc.cohort_id
