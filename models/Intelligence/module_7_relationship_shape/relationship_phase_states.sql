{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'date']
  )
}}

with base as (
  select
    cohort_id,
    date,
    trajectory_emotional_depth
  from {{ ref('relationship_trajectories') }}
),

trust_ctx as (
  select
    cohort_id,
    date,
    trust_level,
    trust_velocity
  from {{ ref('trust_formation_curves') }}
),

expectation_ctx as (
  select
    cohort_id,
    date,
    expectation_violation_magnitude
  from {{ ref('expectation_violation_scores') }}
),

joined as (
  select
    b.cohort_id,
    b.date
  from base b
  left join trust_ctx tc on b.cohort_id = tc.cohort_id and b.date = tc.date
  left join expectation_ctx ec on b.cohort_id = ec.cohort_id and b.date = ec.date
)

select
  cohort_id,
  date,
  cast(null as float64) as phase_awareness_prob,
  cast(null as float64) as phase_curiosity_prob,
  cast(null as float64) as phase_hope_prob,
  cast(null as float64) as phase_evaluation_prob,
  cast(null as float64) as phase_commitment_prob,
  cast(null as float64) as phase_relief_prob,
  cast(null as float64) as phase_satisfaction_prob,
  cast(null as float64) as phase_attachment_prob,
  cast(null as float64) as phase_advocacy_prob,
  cast(null as float64) as phase_drift_prob,
  cast(null as float64) as phase_exit_prob,
  cast(null as float64) as phase_return_prob,
  cast(null as float64) as phase_entropy,
  cast(null as float64) as phase_confidence
from joined
