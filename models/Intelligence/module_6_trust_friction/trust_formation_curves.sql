{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'date']
  )
}}

with cohorts as (
  select distinct cohort_id from {{ ref('psychological_friction_profiles') }}
  union distinct
  select distinct cohort_id from {{ ref('belief_attrition_curves') }}
  union distinct
  select distinct cohort_id from {{ ref('emotional_payoff_curves') }}
  union distinct
  select distinct cohort_id from {{ ref('economic_dissonance_curves') }}
),

dates as (
  select date
  from {{ ref('dim_date') }}
),

cohort_date_spine as (
  select
    c.cohort_id,
    d.date
  from cohorts c
  cross join dates d
),

friction_context as (
  select
    cohort_id,
    friction_trust_intensity,
    friction_coherence,
    friction_volatility
  from {{ ref('psychological_friction_profiles') }}
),

belief_context as (
  select
    cohort_id,
    date,
    belief_retention_index,
    belief_decay_rate,
    stabilization_probability
  from {{ ref('belief_attrition_curves') }}
),

emotional_context as (
  select
    cohort_id,
    date,
    emotional_relief_level,
    emotional_satisfaction_level,
    emotional_regret_level,
    identity_reinforcement_level,
    emotional_hangover_level
  from {{ ref('emotional_payoff_curves') }}
),

dissonance_context as (
  select
    cohort_id,
    date,
    anticipated_value,
    experienced_value,
    reflective_value,
    dissonance_magnitude
  from {{ ref('economic_dissonance_curves') }}
),

base as (
  select
    cds.cohort_id,
    cds.date
  from cohort_date_spine cds
  left join friction_context fc on cds.cohort_id = fc.cohort_id
  left join belief_context bc on cds.cohort_id = bc.cohort_id and cds.date = bc.date
  left join emotional_context ec on cds.cohort_id = ec.cohort_id and cds.date = ec.date
  left join dissonance_context dc on cds.cohort_id = dc.cohort_id and cds.date = dc.date
)

select
  cohort_id,
  date,
  cast(null as float64) as trust_level,
  cast(null as float64) as trust_velocity,
  cast(null as float64) as trust_acceleration
from base
