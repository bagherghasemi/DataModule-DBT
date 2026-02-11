{{
  config(
    materialized='table',
    unique_key='acquisition_source_id'
  )
}}

with sources as (
  select source_id as acquisition_source_id
  from {{ ref('acquisition_sources') }}
),

intake_by_source as (
  select
    acquisition_source_id,
    count(*) as intake_cohort_count
  from {{ ref('acquisition_fingerprints') }}
  group by acquisition_source_id
),

sources_with_intake as (
  select
    s.acquisition_source_id,
    ibs.intake_cohort_count
  from sources s
  left join intake_by_source ibs
    on s.acquisition_source_id = ibs.acquisition_source_id
)

select
  acquisition_source_id,
  cast(null as float64) as identity_reality_alignment,
  cast(null as float64) as loyalty_identity_alignment,
  cast(null as float64) as attachment_identity_alignment,
  cast(null as float64) as brand_fit_index,
  cast(null as string) as brand_identity_reference
from sources_with_intake
