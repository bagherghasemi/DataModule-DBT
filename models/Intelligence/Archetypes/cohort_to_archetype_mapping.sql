{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'archetype_id', 'date']
  )
}}

with cohorts as (
  select distinct cohort_id
  from {{ ref('dim_cohort') }}
),

archetypes as (
  select archetype_id
  from {{ ref('archetype_definitions') }}
),

dates as (
  select date
  from {{ ref('dim_date') }}
),

spine as (
  select
    c.cohort_id,
    a.archetype_id,
    d.date
  from cohorts c
  cross join archetypes a
  cross join dates d
),

module_1_presence as (
  select distinct cohort_id
  from {{ ref('identity_population_profiles') }}
  where cohort_id is not null
),

module_2_presence as (
  select distinct cohort_id
  from {{ ref('intent_field_profiles') }}
  union distinct
  select distinct cohort_id
  from {{ ref('value_field_profiles') }}
),

module_4_presence as (
  select distinct cohort_id
  from {{ ref('motivation_vectors') }}
  union distinct
  select distinct cohort_id
  from {{ ref('psychological_need_profiles') }}
  union distinct
  select distinct cohort_id
  from {{ ref('tension_resolution_paths') }}
),

module_7_presence as (
  select distinct cohort_id, date
  from {{ ref('relationship_trajectories') }}
),

module_8_presence as (
  select distinct cohort_id
  from {{ ref('acquisition_fingerprints') }}
),

evidence_by_cohort_date as (
  select
    s.cohort_id,
    s.archetype_id,
    s.date,
    (select array_agg(x ignore nulls) from unnest([
      if(m1.cohort_id is not null, 'Module 1', cast(null as string)),
      if(m2.cohort_id is not null, 'Module 2', cast(null as string)),
      if(m4.cohort_id is not null, 'Module 4', cast(null as string)),
      if(m7.cohort_id is not null, 'Module 7', cast(null as string)),
      if(m8.cohort_id is not null, 'Module 8', cast(null as string))
    ]) as x) as primary_evidence_modules
  from spine s
  left join module_1_presence m1 on s.cohort_id = m1.cohort_id
  left join module_2_presence m2 on s.cohort_id = m2.cohort_id
  left join module_4_presence m4 on s.cohort_id = m4.cohort_id
  left join module_7_presence m7 on s.cohort_id = m7.cohort_id and s.date = m7.date
  left join module_8_presence m8 on s.cohort_id = m8.cohort_id
),

archetype_count as (
  select count(*) as n
  from archetypes
)

select
  e.cohort_id,
  e.archetype_id,
  e.date,
  cast(1.0 / (select n from archetype_count) as float64) as membership_weight,
  least(1.0, greatest(0.0,
    coalesce(array_length(e.primary_evidence_modules), 0) / 5.0
  )) as membership_confidence,
  coalesce(e.primary_evidence_modules, array<string>[]) as primary_evidence_modules,
  case
    when coalesce(array_length(e.primary_evidence_modules), 0) >= 4 then 'stable'
    else 'provisional'
  end as assignment_status
from evidence_by_cohort_date e
