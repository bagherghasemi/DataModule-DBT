{{
  config(
    materialized='table',
    unique_key='acquisition_source_id'
  )
}}

with sources_from_bfi as (
  select distinct acquisition_source_id
  from {{ ref('brand_fit_indices') }}
),

sources_from_lyr as (
  select distinct acquisition_source_id
  from {{ ref('loyalty_yield_ratios') }}
),

sources_from_trv as (
  select distinct acquisition_source_id
  from {{ ref('toxicity_risk_vectors') }}
),

sources_from_cdv as (
  select distinct acquisition_source_id
  from {{ ref('cultural_drift_vectors') }}
),

sources as (
  select acquisition_source_id from sources_from_bfi
  union distinct
  select acquisition_source_id from sources_from_lyr
  union distinct
  select acquisition_source_id from sources_from_trv
  union distinct
  select acquisition_source_id from sources_from_cdv
),

with_probs as (
  select
    acquisition_source_id,
    cast(0.2 as float64) as growth_compounding_prob,
    cast(0.2 as float64) as growth_neutral_prob,
    cast(0.2 as float64) as growth_extractive_prob,
    cast(0.2 as float64) as growth_corrosive_prob,
    cast(0.2 as float64) as growth_destabilizing_prob
  from sources
)

select
  acquisition_source_id,
  growth_compounding_prob,
  growth_neutral_prob,
  growth_extractive_prob,
  growth_corrosive_prob,
  growth_destabilizing_prob,
  to_json_string(
    struct(
      growth_compounding_prob,
      growth_neutral_prob,
      growth_extractive_prob,
      growth_corrosive_prob,
      growth_destabilizing_prob
    )
  ) as growth_type_distribution
from with_probs
