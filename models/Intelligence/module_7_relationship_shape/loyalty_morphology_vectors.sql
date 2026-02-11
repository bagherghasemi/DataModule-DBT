{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with cohorts_with_trajectory as (
  select distinct cohort_id
  from {{ ref('relationship_trajectories') }}
),

trajectory_shape as (
  select
    cohort_id,
    avg(cast(relational_stability as float64)) as loyalty_stability,
    avg(cast(relationship_volatility as float64)) as loyalty_volatility
  from {{ ref('relationship_trajectories') }}
  where relational_stability is not null or relationship_volatility is not null
  group by cohort_id
),

emotional_persistence as (
  select
    cohort_id,
    cast(decay_velocity as float64) as loyalty_decay_rate,
    cast(reinforcement_sensitivity as float64) as loyalty_recovery_speed
  from {{ ref('emotional_half_life_profiles') }}
),

bonding_dynamics as (
  select
    cohort_id,
    cast(attachment_velocity as float64) as loyalty_growth_rate
  from {{ ref('attachment_velocity_scores') }}
),

base as (
  select cohort_id from cohorts_with_trajectory
)

select
  base.cohort_id,
  ts.loyalty_stability,
  bd.loyalty_growth_rate,
  ts.loyalty_volatility,
  ep.loyalty_recovery_speed,
  ep.loyalty_decay_rate,
  struct(
    ts.loyalty_stability as stability,
    bd.loyalty_growth_rate as growth_rate,
    ts.loyalty_volatility as volatility,
    ep.loyalty_recovery_speed as recovery_speed,
    ep.loyalty_decay_rate as decay_rate
  ) as loyalty_morphology
from base
left join trajectory_shape ts on base.cohort_id = ts.cohort_id
left join emotional_persistence ep on base.cohort_id = ep.cohort_id
left join bonding_dynamics bd on base.cohort_id = bd.cohort_id
