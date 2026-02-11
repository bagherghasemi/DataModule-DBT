{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'from_population_stage', 'to_population_stage']
  )
}}

with cohort_space as (
  select distinct
    cohort_id
  from {{ ref('dim_cohort') }}
),

population_stage_space as (
  select
    population_stage
  from {{ ref('enum_population_stage') }}
),

from_stage_space as (
  select
    cs.cohort_id,
    pss.population_stage as from_population_stage
  from cohort_space cs
  cross join population_stage_space pss
),

full_transition_space as (
  select
    fss.cohort_id,
    fss.from_population_stage,
    pss.population_stage as to_population_stage
  from from_stage_space fss
  cross join population_stage_space pss
)

select
  cohort_id,
  from_population_stage,
  to_population_stage,
  cast(null as numeric) as transition_probability,
  cast(null as numeric) as composition_delta,
  cast(null as numeric) as leakage_intensity
from full_transition_space
