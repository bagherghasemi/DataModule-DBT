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

trust_decay as (
  select
    cohort_id,
    collapse_speed
  from {{ ref('trust_decay_signatures') }}
),

belief_attrition_agg as (
  select
    cohort_id,
    avg(stabilization_probability) as avg_stabilization_probability
  from {{ ref('belief_attrition_curves') }}
  where stabilization_probability is not null
  group by cohort_id
),

regret_formation as (
  select distinct cohort_id from {{ ref('regret_formation_indices') }}
),

base as (
  select
    c.cohort_id,
    tds.collapse_speed,
    bac.avg_stabilization_probability
  from cohorts c
  left join trust_decay tds on c.cohort_id = tds.cohort_id
  left join belief_attrition_agg bac on c.cohort_id = bac.cohort_id
  left join regret_formation rfi on c.cohort_id = rfi.cohort_id
)

select
  cohort_id,
  cast(null as float64) as emotional_half_life,
  greatest(0, coalesce(collapse_speed, 0)) as decay_velocity,
  case
    when avg_stabilization_probability is not null then least(1.0, greatest(0.0, avg_stabilization_probability))
    else null
  end as reinforcement_sensitivity
from base
