{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'date']
  )
}}

with spine as (
  select
    cohort_id,
    date
  from {{ ref('belief_attrition_curves') }}
),

bac as (
  select
    cohort_id,
    date,
    belief_retention_index,
    belief_decay_rate,
    stabilization_probability
  from {{ ref('belief_attrition_curves') }}
),

ysc as (
  select
    cohort_id,
    date,
    expressed_desire,
    realized_value,
    relationship_stability,
    human_yield
  from {{ ref('yield_stability_curves') }}
),

itm_by_cohort as (
  select
    cohort_id,
    avg(transition_probability) as avg_transition_probability,
    avg(leakage_intensity) as avg_leakage_intensity
  from {{ ref('identity_transition_matrix') }}
  group by cohort_id
),

base as (
  select
    s.cohort_id,
    s.date,
    bac.belief_decay_rate,
    bac.stabilization_probability,
    ysc.relationship_stability,
    ysc.human_yield,
    itm.avg_transition_probability,
    itm.avg_leakage_intensity
  from spine s
  left join bac
    on s.cohort_id = bac.cohort_id and s.date = bac.date
  left join ysc
    on s.cohort_id = ysc.cohort_id and s.date = ysc.date
  left join itm_by_cohort itm
    on s.cohort_id = itm.cohort_id
),

with_relief as (
  select
    cohort_id,
    date,
    least(1.0, greatest(0.0, coalesce(stabilization_probability, relationship_stability, 0.0))) as emotional_relief_level,
    least(1.0, greatest(0.0, coalesce(human_yield, 0.0))) as emotional_satisfaction_level,
    least(1.0, greatest(0.0, coalesce(avg_leakage_intensity, 0.0))) as emotional_regret_level,
    least(1.0, greatest(0.0, coalesce(avg_transition_probability, 0.0))) as identity_reinforcement_level,
    least(1.0, greatest(0.0, coalesce(belief_decay_rate, 0.0))) as emotional_hangover_level
  from base
)

select
  cohort_id,
  date,
  emotional_relief_level,
  emotional_satisfaction_level,
  emotional_regret_level,
  identity_reinforcement_level,
  emotional_hangover_level,
  struct(
    emotional_relief_level,
    emotional_satisfaction_level,
    emotional_regret_level,
    identity_reinforcement_level,
    emotional_hangover_level
  ) as emotional_payoff_curve
from with_relief
