{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with belief_by_cohort as (
  select
    cohort_id,
    avg(belief_retention_index) as avg_belief_retention_index,
    avg(belief_decay_rate) as avg_belief_decay_rate,
    avg(stabilization_probability) as avg_stabilization_probability
  from {{ ref('belief_attrition_curves') }}
  group by cohort_id
),

base as (
  select pnp.cohort_id
  from {{ ref('psychological_need_profiles') }} pnp
),

with_needs as (
  select
    base.cohort_id,
    pnp.unmet_need_pressure,
    pnp.need_coherence,
    pnp.psychological_fragility,
    pnp.psychological_need_profile
  from base
  left join {{ ref('psychological_need_profiles') }} pnp
    on base.cohort_id = pnp.cohort_id
),

with_belief as (
  select
    wn.cohort_id,
    wn.unmet_need_pressure,
    wn.need_coherence,
    wn.psychological_fragility,
    wn.psychological_need_profile,
    bc.avg_belief_retention_index,
    bc.avg_belief_decay_rate,
    bc.avg_stabilization_probability
  from with_needs wn
  left join belief_by_cohort bc
    on wn.cohort_id = bc.cohort_id
),

with_value as (
  select
    wb.cohort_id,
    wb.unmet_need_pressure,
    wb.need_coherence,
    wb.psychological_fragility,
    wb.psychological_need_profile,
    wb.avg_belief_retention_index,
    wb.avg_belief_decay_rate,
    wb.avg_stabilization_probability,
    vfp.friction_load,
    vfp.regret_load,
    vfp.support_cost_load,
    vfp.realized_value,
    vfp.value_coherence,
    vfp.value_volatility
  from with_belief wb
  left join {{ ref('value_field_profiles') }} vfp
    on wb.cohort_id = vfp.cohort_id
),

final_base as (
  select wv.*
  from with_value wv
  left join (select 1 as _dummy from {{ ref('narrative_concordance_scores') }} limit 1) _narrative_ref
    on true
)

select
  final_base.cohort_id,

  least(1.0, greatest(0.0, cast(null as float64))) as tension_status_insecurity,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_belonging_anxiety,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_control_loss,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_overwhelm,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_fear_of_regret,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_desire_for_meaning,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_desire_for_pleasure,
  least(1.0, greatest(0.0, cast(null as float64))) as tension_need_for_certainty,

  least(1.0, greatest(0.0, cast(null as float64))) as tension_resolution_effectiveness,
  least(1.0, greatest(0.0, cast(null as float64))) as emotional_dependency_risk,
  least(1.0, greatest(0.0, cast(null as float64))) as autonomy_reinforcement_score,

  struct(
    cast(null as float64) as tension_status_insecurity,
    cast(null as float64) as tension_belonging_anxiety,
    cast(null as float64) as tension_control_loss,
    cast(null as float64) as tension_overwhelm,
    cast(null as float64) as tension_fear_of_regret,
    cast(null as float64) as tension_desire_for_meaning,
    cast(null as float64) as tension_desire_for_pleasure,
    cast(null as float64) as tension_need_for_certainty,
    cast(null as float64) as tension_resolution_effectiveness,
    cast(null as float64) as emotional_dependency_risk,
    cast(null as float64) as autonomy_reinforcement_score
  ) as tension_resolution_path

from final_base
