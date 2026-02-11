{{
  config(
    materialized='table'
  )
}}

with cohorts as (
  select distinct
    cohort_id
  from {{ ref('dim_cohort') }}
),

dates as (
  select
    date
  from {{ ref('dim_date') }}
)

select
  c.cohort_id,
  d.date,
  cast(null as string) as attrition_phase,
  cast(null as string) as trigger_type,
  cast(null as float64) as belief_retention_index,
  cast(null as float64) as belief_decay_rate,
  cast(null as float64) as stabilization_probability
from cohorts c
cross join dates d
