{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with source_cohorts as (
  select distinct
    cohort_id
  from {{ ref('dim_cohort') }}
)

select
  cohort_id,
  cast(null as float64) as drift_magnitude,
  cast(null as float64) as drift_velocity,
  cast(null as float64) as coherence_loss,
  cast(null as string) as dominant_drift_axis,
  cast(null as string) as drift_type_label
from source_cohorts
