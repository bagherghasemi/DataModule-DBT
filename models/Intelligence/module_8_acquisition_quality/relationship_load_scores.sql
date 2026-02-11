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

with_friction as (
  select
    sc.acquisition_source_id,
    sc.cohort_id,
    pfp.friction_trust_intensity,
    pfp.friction_risk_intensity,
    pfp.friction_cognitive_intensity,
    pfp.friction_identity_intensity,
    pfp.friction_control_intensity,
    pfp.friction_expectation_intensity
  from source_cohorts sc
  left join {{ ref('psychological_friction_profiles') }} pfp
    on sc.cohort_id = pfp.cohort_id
),

with_trust as (
  select
    wf.acquisition_source_id,
    wf.cohort_id,
    wf.friction_trust_intensity,
    wf.friction_risk_intensity,
    wf.friction_cognitive_intensity,
    wf.friction_identity_intensity,
    wf.friction_control_intensity,
    wf.friction_expectation_intensity,
    tds.trust_fragility_index
  from with_friction wf
  left join {{ ref('trust_decay_signatures') }} tds
    on wf.cohort_id = tds.cohort_id
),

with_regret as (
  select
    wt.acquisition_source_id,
    wt.cohort_id,
    wt.friction_trust_intensity,
    wt.friction_risk_intensity,
    wt.friction_cognitive_intensity,
    wt.friction_identity_intensity,
    wt.friction_control_intensity,
    wt.friction_expectation_intensity,
    wt.trust_fragility_index,
    rfi.regret_probability,
    rfi.regret_persistence
  from with_trust wt
  left join {{ ref('regret_formation_indices') }} rfi
    on wt.cohort_id = rfi.cohort_id
),

emotional_half_life_joined as (
  select
    wr.acquisition_source_id,
    wr.cohort_id,
    wr.friction_trust_intensity,
    wr.friction_risk_intensity,
    wr.friction_cognitive_intensity,
    wr.friction_identity_intensity,
    wr.friction_control_intensity,
    wr.friction_expectation_intensity,
    wr.trust_fragility_index,
    wr.regret_probability,
    wr.regret_persistence,
    ehl.emotional_half_life,
    ehl.decay_velocity,
    ehl.reinforcement_sensitivity
  from with_regret wr
  left join {{ ref('emotional_half_life_profiles') }} ehl
    on wr.cohort_id = ehl.cohort_id
),

agg as (
  select
    acquisition_source_id,
    cast(null as float64) as support_burden,
    avg(
      case
        when friction_trust_intensity is not null
          or friction_risk_intensity is not null
          or friction_cognitive_intensity is not null
          or friction_identity_intensity is not null
          or friction_control_intensity is not null
          or friction_expectation_intensity is not null
        then (
          coalesce(friction_trust_intensity, 0)
          + coalesce(friction_risk_intensity, 0)
          + coalesce(friction_cognitive_intensity, 0)
          + coalesce(friction_identity_intensity, 0)
          + coalesce(friction_control_intensity, 0)
          + coalesce(friction_expectation_intensity, 0)
        ) / 6.0
        else null
      end
    ) as friction_burden,
    avg(trust_fragility_index) as trust_fragility,
    avg(regret_probability) as regret_density,
    avg(friction_cognitive_intensity) as cognitive_burden
  from emotional_half_life_joined
  group by acquisition_source_id
),

scored as (
  select
    a.acquisition_source_id,
    a.support_burden,
    a.friction_burden,
    a.trust_fragility,
    a.regret_density,
    a.cognitive_burden,
    case
      when a.support_burden is null
        and a.friction_burden is null
        and a.trust_fragility is null
        and a.regret_density is null
        and a.cognitive_burden is null
      then cast(null as float64)
      else least(
        1.0,
        greatest(
          0.0,
          (
            coalesce(a.support_burden, 0)
            + coalesce(a.friction_burden, 0)
            + coalesce(a.trust_fragility, 0)
            + coalesce(a.regret_density, 0)
            + coalesce(a.cognitive_burden, 0)
          )
          / nullif(
            (if(a.support_burden is not null, 1, 0) + if(a.friction_burden is not null, 1, 0) + if(a.trust_fragility is not null, 1, 0) + if(a.regret_density is not null, 1, 0) + if(a.cognitive_burden is not null, 1, 0)),
            0
          )
        )
      )
    end as relationship_load_score
  from agg a
)

select
  s.acquisition_source_id,
  sc.support_burden,
  sc.friction_burden,
  sc.trust_fragility,
  sc.regret_density,
  sc.cognitive_burden,
  sc.relationship_load_score
from sources s
left join scored sc
  on s.acquisition_source_id = sc.acquisition_source_id
