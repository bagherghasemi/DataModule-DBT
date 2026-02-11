{{
  config(
    materialized='table',
    unique_key='acquisition_source_id'
  )
}}

with gtd_sources as (
  select distinct acquisition_source_id
  from {{ ref('growth_type_distributions') }}
),

bfi_sources as (
  select distinct acquisition_source_id
  from {{ ref('brand_fit_indices') }}
),

rls_sources as (
  select distinct acquisition_source_id
  from {{ ref('relationship_load_scores') }}
),

trv_sources as (
  select distinct acquisition_source_id
  from {{ ref('toxicity_risk_vectors') }}
),

lyr_sources as (
  select distinct acquisition_source_id
  from {{ ref('loyalty_yield_ratios') }}
),

anchor as (
  select acquisition_source_id from gtd_sources
  union distinct
  select acquisition_source_id from bfi_sources
  union distinct
  select acquisition_source_id from rls_sources
  union distinct
  select acquisition_source_id from trv_sources
  union distinct
  select acquisition_source_id from lyr_sources
),

joined as (
  select
    a.acquisition_source_id,
    gtd.growth_compounding_prob,
    gtd.growth_neutral_prob,
    gtd.growth_extractive_prob,
    gtd.growth_corrosive_prob,
    gtd.growth_destabilizing_prob,
    bfi.brand_fit_index,
    rls.relationship_load_score,
    trv.toxicity_regret_pressure,
    trv.toxicity_friction_pressure,
    trv.toxicity_identity_misalignment,
    trv.toxicity_support_burden,
    trv.toxicity_instability,
    lyr.loyalty_yield_ratio
  from anchor a
  left join {{ ref('growth_type_distributions') }} gtd
    on a.acquisition_source_id = gtd.acquisition_source_id
  left join {{ ref('brand_fit_indices') }} bfi
    on a.acquisition_source_id = bfi.acquisition_source_id
  left join {{ ref('relationship_load_scores') }} rls
    on a.acquisition_source_id = rls.acquisition_source_id
  left join {{ ref('toxicity_risk_vectors') }} trv
    on a.acquisition_source_id = trv.acquisition_source_id
  left join {{ ref('loyalty_yield_ratios') }} lyr
    on a.acquisition_source_id = lyr.acquisition_source_id
),

with_dims as (
  select
    acquisition_source_id,
    brand_fit_index as identity_fit_contribution,
    relationship_load_score as relationship_load_pressure,
    case
      when toxicity_regret_pressure is null
        and toxicity_friction_pressure is null
        and toxicity_identity_misalignment is null
        and toxicity_support_burden is null
        and toxicity_instability is null
      then cast(null as float64)
      else least(
        1.0,
        greatest(
          0.0,
          (
            coalesce(least(1.0, greatest(0.0, toxicity_regret_pressure)), 0)
            + coalesce(least(1.0, greatest(0.0, toxicity_friction_pressure)), 0)
            + coalesce(least(1.0, greatest(0.0, toxicity_identity_misalignment)), 0)
            + coalesce(least(1.0, greatest(0.0, toxicity_support_burden)), 0)
            + coalesce(least(1.0, greatest(0.0, toxicity_instability)), 0)
          )
          / nullif(
            (if(toxicity_regret_pressure is not null, 1, 0)
             + if(toxicity_friction_pressure is not null, 1, 0)
             + if(toxicity_identity_misalignment is not null, 1, 0)
             + if(toxicity_support_burden is not null, 1, 0)
             + if(toxicity_instability is not null, 1, 0)),
            0
          )
        )
      )
    end as toxicity_drag,
    loyalty_yield_ratio as loyalty_yield_quality,
    growth_compounding_prob as growth_type_alignment
  from joined
),

with_score as (
  select
    acquisition_source_id,
    identity_fit_contribution,
    relationship_load_pressure,
    toxicity_drag,
    loyalty_yield_quality,
    growth_type_alignment,
    case
      when identity_fit_contribution is null
        and relationship_load_pressure is null
        and toxicity_drag is null
        and loyalty_yield_quality is null
        and growth_type_alignment is null
      then cast(null as float64)
      else least(
        1.0,
        greatest(
          0.0,
          (
            coalesce(least(1.0, greatest(0.0, identity_fit_contribution)), 0)
            + coalesce(1.0 - least(1.0, greatest(0.0, relationship_load_pressure)), 0)
            + coalesce(1.0 - toxicity_drag, 0)
            + coalesce(least(1.0, greatest(0.0, loyalty_yield_quality)), 0)
            + coalesce(least(1.0, greatest(0.0, growth_type_alignment)), 0)
          )
          / nullif(
            (if(identity_fit_contribution is not null, 1, 0)
             + if(relationship_load_pressure is not null, 1, 0)
             + if(toxicity_drag is not null, 1, 0)
             + if(loyalty_yield_quality is not null, 1, 0)
             + if(growth_type_alignment is not null, 1, 0)),
            0
          )
        )
      )
    end as compounding_potential_score
  from with_dims
)

select
  acquisition_source_id,
  identity_fit_contribution,
  relationship_load_pressure,
  toxicity_drag,
  loyalty_yield_quality,
  growth_type_alignment,
  compounding_potential_score
from with_score
