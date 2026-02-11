{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with cohorts_with_mapping as (
  select
    dvm.cohort_id,
    dvm.desire_coordinate,
    dvm.value_coordinate
  from {{ ref('desire_value_map') }} dvm
),

aligned_profiles as (
  select
    cwm.cohort_id,
    cwm.desire_coordinate,
    cwm.value_coordinate,
    ifp.desire_volatility,
    vfp.value_volatility
  from cohorts_with_mapping cwm
  left join {{ ref('intent_field_profiles') }} ifp
    on cwm.cohort_id = ifp.cohort_id
  left join {{ ref('value_field_profiles') }} vfp
    on cwm.cohort_id = vfp.cohort_id
),

differential as (
  select
    cohort_id,
    desire_coordinate,
    value_coordinate,
    abs(desire_coordinate - value_coordinate) as desire_reciprocity_differential_magnitude,
    case
      when abs(desire_coordinate - value_coordinate) > 0
        then (desire_coordinate - value_coordinate) / abs(desire_coordinate - value_coordinate)
      else 0.0
    end as desire_reciprocity_differential_direction,
    coalesce(desire_volatility, 0) - coalesce(value_volatility, 0) as imbalance_velocity
  from aligned_profiles
)

select
  cohort_id,
  desire_reciprocity_differential_magnitude,
  desire_reciprocity_differential_direction,
  imbalance_velocity
from differential
