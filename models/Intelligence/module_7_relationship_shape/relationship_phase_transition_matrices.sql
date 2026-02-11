{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'phase_from', 'phase_to', 'date']
  )
}}

with cohort_dates as (
  select distinct
    cohort_id,
    date
  from {{ ref('relationship_phase_states') }}
),

phases as (
  select phase_name
  from unnest([
    'awareness', 'curiosity', 'hope', 'evaluation', 'commitment', 'relief',
    'satisfaction', 'attachment', 'advocacy', 'drift', 'exit', 'return'
  ]) as phase_name
),

phase_pairs as (
  select
    p_from.phase_name as phase_from,
    p_to.phase_name as phase_to
  from phases p_from
  cross join phases p_to
),

spine as (
  select
    cd.cohort_id,
    cd.date,
    pp.phase_from,
    pp.phase_to
  from cohort_dates cd
  cross join phase_pairs pp
),

rfi as (
  select cohort_id
  from {{ ref('regret_formation_indices') }}
),

tds as (
  select cohort_id
  from {{ ref('trust_decay_signatures') }}
),

joined as (
  select
    s.cohort_id,
    s.date,
    s.phase_from,
    s.phase_to
  from spine s
  left join rfi on s.cohort_id = rfi.cohort_id
  left join tds on s.cohort_id = tds.cohort_id
)

select
  j.cohort_id,
  j.phase_from,
  j.phase_to,
  j.date,
  cast(null as float64) as transition_probability,
  cast(null as float64) as transition_velocity
from joined j
