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

dissonance as (
  select
    cohort_id,
    date,
    dissonance_magnitude
  from {{ ref('economic_dissonance_curves') }}
),

base as (
  select
    s.cohort_id,
    s.date,
    d.dissonance_magnitude
  from spine s
  left join dissonance d
    on s.cohort_id = d.cohort_id and s.date = d.date
)

select
  cohort_id,
  date,
  cast(null as string) as creative_cluster,
  case
    when dissonance_magnitude is not null
    then least(1.0, greatest(0.0, dissonance_magnitude))
    else null
  end as expectation_violation_magnitude,
  cast(null as float64) as expectation_fragility,
  cast(null as float64) as expectation_repairability
from base
