{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with motivation_base as (
  select
    cohort_id
  from {{ ref('motivation_vectors') }}
),

emotional_dependency as (
  select distinct
    cohort_id
  from {{ ref('emotional_payoff_curves') }}
),

identity_dependency as (
  select distinct
    cohort_id
  from {{ ref('identity_transition_matrix') }}
),

cohort_spine as (
  select distinct
    mb.cohort_id
  from motivation_base mb
  left join emotional_dependency ed
    on mb.cohort_id = ed.cohort_id
  left join identity_dependency idm
    on mb.cohort_id = idm.cohort_id
)

select
  cohort_spine.cohort_id,
  cast(null as numeric) as motivation_stability_index,
  cast(null as numeric) as motivation_volatility,
  cast(null as numeric) as drift_sensitivity,
  struct(
    cast(null as numeric) as motivation_stability_index,
    cast(null as numeric) as motivation_volatility,
    cast(null as numeric) as drift_sensitivity
  ) as motivation_stability_signature
from cohort_spine