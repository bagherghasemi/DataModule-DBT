{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with value_context as (
  select cohort_id
  from {{ ref('value_interpretation_profiles') }}
),

motivation_context as (
  select cohort_id
  from {{ ref('motivation_vectors') }}
)

select
  vc.cohort_id,

  cast(null as numeric) as price_as_danger_intensity,
  cast(null as numeric) as price_as_reassurance_intensity,
  cast(null as numeric) as price_as_uncertainty_intensity,

  cast(null as numeric) as risk_encoding_coherence,
  cast(null as string) as risk_encoding_signature

from value_context vc
left join motivation_context mc
  on vc.cohort_id = mc.cohort_id
