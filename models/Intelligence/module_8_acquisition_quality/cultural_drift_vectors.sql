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

source_cohort_vectors as (
  select
    af.acquisition_source_id,
    af.cohort_id,
    idv.drift_magnitude as idv_drift_magnitude,
    idv.drift_velocity,
    idv.coherence_loss,
    idv.dominant_drift_axis,
    idv.drift_type_label,
    pmv.price_meaning_vector,
    lmv.loyalty_morphology
  from {{ ref('acquisition_fingerprints') }} af
  left join {{ ref('identity_drift_vectors') }} idv
    on af.cohort_id = idv.cohort_id
  left join {{ ref('price_meaning_vectors') }} pmv
    on af.cohort_id = pmv.cohort_id
  left join {{ ref('loyalty_morphology_vectors') }} lmv
    on af.cohort_id = lmv.cohort_id
),

agg_by_source as (
  select
    acquisition_source_id,
    struct(
      sum(idv_drift_magnitude) as drift_magnitude,
      sum(drift_velocity) as drift_velocity,
      sum(coherence_loss) as coherence_loss,
      any_value(dominant_drift_axis) as dominant_drift_axis,
      any_value(drift_type_label) as drift_type_label
    ) as drift_identity_direction,
    struct(
      avg(price_meaning_vector.price_meaning_smart_choice_intensity) as price_meaning_smart_choice_intensity,
      avg(price_meaning_vector.price_meaning_investing_in_self_intensity) as price_meaning_investing_in_self_intensity,
      avg(price_meaning_vector.price_meaning_exploitation_intensity) as price_meaning_exploitation_intensity,
      avg(price_meaning_vector.price_meaning_risk_intensity) as price_meaning_risk_intensity,
      avg(price_meaning_vector.price_meaning_safety_intensity) as price_meaning_safety_intensity,
      avg(price_meaning_vector.price_meaning_treat_intensity) as price_meaning_treat_intensity,
      avg(price_meaning_vector.price_meaning_identity_intensity) as price_meaning_identity_intensity
    ) as drift_price_norm_direction,
    struct(
      avg(loyalty_morphology.stability) as stability,
      avg(loyalty_morphology.growth_rate) as growth_rate,
      avg(loyalty_morphology.volatility) as volatility,
      avg(loyalty_morphology.recovery_speed) as recovery_speed,
      avg(loyalty_morphology.decay_rate) as decay_rate
    ) as drift_relationship_shape_direction,
    sum(idv_drift_magnitude) as drift_magnitude,
    cast(null as float64) as drift_acceleration
  from source_cohort_vectors
  group by acquisition_source_id
)

select
  s.acquisition_source_id,
  a.drift_identity_direction,
  cast(null as string) as drift_expectation_direction,
  a.drift_price_norm_direction,
  cast(null as string) as drift_trust_norm_direction,
  a.drift_relationship_shape_direction,
  a.drift_magnitude,
  a.drift_acceleration
from sources s
left join agg_by_source a
  on s.acquisition_source_id = a.acquisition_source_id
