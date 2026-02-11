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

epc_cohorts as (
  select distinct cohort_id
  from {{ ref('emotional_payoff_curves') }}
),

edc_persistence as (
  select
    cohort_id,
    avg(dissonance_magnitude) as avg_dissonance
  from {{ ref('economic_dissonance_curves') }}
  where dissonance_magnitude is not null
  group by cohort_id
),

base as (
  select
    c.cohort_id,
    edc.avg_dissonance
  from cohorts c
  left join epc_cohorts epc on c.cohort_id = epc.cohort_id
  left join edc_persistence edc on c.cohort_id = edc.cohort_id
)

select
  cohort_id,
  cast(null as float64) as regret_probability,
  cast(null as float64) as regret_latency,
  case
    when avg_dissonance is not null then least(1.0, greatest(0.0, avg_dissonance))
    else null
  end as regret_persistence
from base
