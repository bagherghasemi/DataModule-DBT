{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'date']
  )
}}

with cohorts as (
  select distinct cohort_id from {{ ref('trust_formation_curves') }}
  union distinct
  select distinct cohort_id from {{ ref('psychological_friction_profiles') }}
  union distinct
  select distinct cohort_id from {{ ref('belief_attrition_curves') }}
  union distinct
  select distinct cohort_id from {{ ref('emotional_payoff_curves') }}
  union distinct
  select distinct cohort_id from {{ ref('desire_value_map') }}
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

trust_signals as (
  select
    cohort_id,
    date,
    trust_level,
    trust_velocity
  from {{ ref('trust_formation_curves') }}
),

friction_signals as (
  select
    cohort_id,
    friction_volatility
  from {{ ref('psychological_friction_profiles') }}
),

belief_signals as (
  select
    cohort_id,
    date,
    belief_retention_index,
    stabilization_probability
  from {{ ref('belief_attrition_curves') }}
),

emotional_signals as (
  select
    cohort_id,
    date,
    identity_reinforcement_level
  from {{ ref('emotional_payoff_curves') }}
),

desire_signals as (
  select
    cohort_id,
    desire_coordinate
  from {{ ref('desire_value_map') }}
),

base as (
  select cohort_id, date from cohort_date_spine
)

select
  base.cohort_id,
  base.date,
  cast(es.identity_reinforcement_level as float64) as trajectory_emotional_depth,
  cast(ts.trust_level as float64) as trajectory_trust,
  cast(bs.belief_retention_index as float64) as trajectory_identity_alignment,
  cast(ds.desire_coordinate as float64) as trajectory_desire_stability,
  cast(fs.friction_volatility as float64) as trajectory_friction_pressure,
  cast(bs.stabilization_probability as float64) as relational_stability,
  cast(ts.trust_velocity as float64) as relationship_volatility
from base
left join trust_signals ts on base.cohort_id = ts.cohort_id and base.date = ts.date
left join friction_signals fs on base.cohort_id = fs.cohort_id
left join belief_signals bs on base.cohort_id = bs.cohort_id and base.date = bs.date
left join emotional_signals es on base.cohort_id = es.cohort_id and base.date = es.date
left join desire_signals ds on base.cohort_id = ds.cohort_id
