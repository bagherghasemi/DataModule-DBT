{{ 
  config(
    materialized='table',
    unique_key='cohort_id'
  ) 
}}

with cohorts as (
  select distinct
    cohort_id
  from {{ ref('dim_cohort') }}
)

select
  c.cohort_id,
  cast(null as string)  as creative_cluster_id,
  cast(null as string)  as implied_promise_signature,
  cast(null as string)  as expectation_frame,
  cast(null as float64) as promise_survival_probability,
  cast(null as float64) as expectation_violation_rate,
  cast(null as float64) as emotional_dissonance
from cohorts c
