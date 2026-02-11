{{
  config(
    materialized='table'
  )
}}

select
  ad.archetype_id,

  -- Module 1: Identity & Reality Gap
  cast(null as string) as identity_alignment_tendency,
  cast(null as string) as reality_gap_sensitivity,

  -- Module 2: Intent vs Value
  cast(null as string) as intent_dominance_tendency,
  cast(null as string) as value_extraction_tendency,
  cast(null as string) as reciprocity_balance_tendency,

  -- Module 3: Creative Truth
  cast(null as string) as narrative_concordance_dependence,
  cast(null as string) as recruitment_selectivity,
  cast(null as string) as story_yield_responsiveness,

  -- Module 4: Motivation Psychology
  cast(null as string) as motivation_coherence_need,
  cast(null as string) as psychological_need_rigidity,
  cast(null as string) as tension_resolution_style,

  -- Module 5: Price Meaning
  cast(null as string) as price_signal_interpretation_style,
  cast(null as string) as discount_sensitivity_tendency,
  cast(null as string) as risk_encoding_bias,

  -- Module 6: Trust & Friction
  cast(null as string) as trust_formation_velocity,
  cast(null as string) as trust_decay_susceptibility,
  cast(null as string) as friction_tolerance_level,
  cast(null as string) as regret_formation_risk,

  -- Module 7: Relationship Shape
  cast(null as string) as attachment_velocity_profile,
  cast(null as string) as relationship_stability_preference,
  cast(null as string) as return_elasticity_pattern,

  -- Module 8: Acquisition Quality
  cast(null as string) as acquisition_load_tolerance,
  cast(null as string) as cultural_drift_contribution,
  cast(null as string) as loyalty_yield_efficiency,
  cast(null as string) as toxicity_risk_contribution
from {{ ref('archetype_definitions') }} ad
