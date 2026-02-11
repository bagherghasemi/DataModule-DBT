{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with cohorts_in_dim as (
  select distinct cohort_id
  from {{ ref('dim_cohort') }}
),

aligned_profiles as (
  select
    ifp.cohort_id,
    ifp.expressed_desire,
    vfp.realized_value
  from {{ ref('intent_field_profiles') }} ifp
  inner join {{ ref('value_field_profiles') }} vfp
    on ifp.cohort_id = vfp.cohort_id
  inner join cohorts_in_dim cid
    on ifp.cohort_id = cid.cohort_id
),

positioning_raw as (
  select
    cohort_id,
    expressed_desire as desire_coordinate,
    least(1.0, greatest(0.0, realized_value)) as value_coordinate
  from aligned_profiles
),

with_diff as (
  select
    cohort_id,
    desire_coordinate,
    value_coordinate,
    desire_coordinate - value_coordinate as position_diff
  from positioning_raw
),

zone_assignment as (
  select
    cohort_id,
    desire_coordinate,
    value_coordinate,
    case
      when ntile(4) over (order by position_diff, cohort_id) = 1 then 'hidden_compounding'
      when ntile(4) over (order by position_diff, cohort_id) = 2 then 'stability'
      when ntile(4) over (order by position_diff, cohort_id) = 3 then 'illusion'
      when ntile(4) over (order by position_diff, cohort_id) = 4 then 'extraction'
    end as structural_zone_label
  from with_diff
)

select
  cohort_id,
  desire_coordinate,
  value_coordinate,
  structural_zone_label
from zone_assignment
