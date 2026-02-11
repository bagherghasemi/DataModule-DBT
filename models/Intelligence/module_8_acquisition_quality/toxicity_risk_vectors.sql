{{
  config(
    materialized='table',
    unique_key='acquisition_source_id'
  )
}}

with sources as (
  select distinct acquisition_source_id
  from {{ ref('acquisition_fingerprints') }}
),

with_load as (
  select
    s.acquisition_source_id,
    rls.regret_density,
    rls.friction_burden,
    rls.support_burden
  from sources s
  left join {{ ref('relationship_load_scores') }} rls
    on s.acquisition_source_id = rls.acquisition_source_id
),

with_yield as (
  select
    wl.acquisition_source_id,
    wl.regret_density,
    wl.friction_burden,
    wl.support_burden,
    lyr.loyalty_yield_ratio
  from with_load wl
  left join {{ ref('loyalty_yield_ratios') }} lyr
    on wl.acquisition_source_id = lyr.acquisition_source_id
),

elasticity_by_source as (
  select
    af.acquisition_source_id,
    avg(rei.return_elasticity_score) as avg_return_elasticity
  from {{ ref('acquisition_fingerprints') }} af
  left join {{ ref('return_elasticity_indices') }} rei
    on af.cohort_id = rei.cohort_id
  group by af.acquisition_source_id
),

with_elasticity as (
  select
    wy.acquisition_source_id,
    wy.regret_density,
    wy.friction_burden,
    wy.support_burden,
    wy.loyalty_yield_ratio,
    ebs.avg_return_elasticity
  from with_yield wy
  left join elasticity_by_source ebs
    on wy.acquisition_source_id = ebs.acquisition_source_id
),

with_drift as (
  select
    we.acquisition_source_id,
    we.regret_density,
    we.friction_burden,
    we.support_burden,
    we.loyalty_yield_ratio,
    we.avg_return_elasticity,
    cdv.drift_magnitude
  from with_elasticity we
  left join {{ ref('cultural_drift_vectors') }} cdv
    on we.acquisition_source_id = cdv.acquisition_source_id
)

select
  acquisition_source_id,
  regret_density as toxicity_regret_pressure,
  friction_burden as toxicity_friction_pressure,
  drift_magnitude as toxicity_identity_misalignment,
  support_burden as toxicity_support_burden,
  cast(null as float64) as toxicity_instability
from with_drift
