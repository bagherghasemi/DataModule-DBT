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

source_cohorts as (
  select
    af.acquisition_source_id,
    af.cohort_id
  from {{ ref('acquisition_fingerprints') }} af
),

with_loyalty as (
  select
    sc.acquisition_source_id,
    sc.cohort_id,
    lmv.loyalty_stability,
    lmv.loyalty_recovery_speed
  from source_cohorts sc
  left join {{ ref('loyalty_morphology_vectors') }} lmv
    on sc.cohort_id = lmv.cohort_id
),

with_return as (
  select
    wl.acquisition_source_id,
    wl.cohort_id,
    wl.loyalty_stability,
    wl.loyalty_recovery_speed,
    rei.return_elasticity_score
  from with_loyalty wl
  left join {{ ref('return_elasticity_indices') }} rei
    on wl.cohort_id = rei.cohort_id
),

with_motivation as (
  select
    wr.acquisition_source_id,
    wr.cohort_id,
    wr.loyalty_stability,
    wr.loyalty_recovery_speed,
    wr.return_elasticity_score,
    msi.motivation_stability_index
  from with_return wr
  left join {{ ref('motivation_stability_indices') }} msi
    on wr.cohort_id = msi.cohort_id
),

agg as (
  select
    acquisition_source_id,
    avg(loyalty_stability) as average_human_yield,
    avg(motivation_stability_index) as yield_stability,
    avg(loyalty_recovery_speed) as compounding_rate
  from with_motivation
  group by acquisition_source_id
),

with_load as (
  select
    s.acquisition_source_id,
    a.average_human_yield,
    a.yield_stability,
    a.compounding_rate,
    rls.relationship_load_score
  from sources s
  left join agg a on s.acquisition_source_id = a.acquisition_source_id
  left join {{ ref('relationship_load_scores') }} rls
    on s.acquisition_source_id = rls.acquisition_source_id
)

select
  acquisition_source_id,
  average_human_yield,
  yield_stability,
  compounding_rate,
  case
    when average_human_yield is not null
      and relationship_load_score is not null
      and relationship_load_score > 0
    then average_human_yield / relationship_load_score
    else cast(null as float64)
  end as loyalty_yield_ratio
from with_load
