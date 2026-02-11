{{
  config(
    materialized='table',
    unique_key='creative_id'
  )
}}

with creatives_from_narrative as (
  select distinct ncs.creative_id
  from {{ ref('narrative_concordance_scores') }} ncs
  where ncs.creative_id is not null
),

narrative_structural as (
  select
    creative_id,
    expectation_violation_rate,
    lived_experience_divergence,
    narrative_concordance_score
  from {{ ref('narrative_concordance_scores') }}
  where creative_id is not null
),

story_yield_agg as (
  select
    creative_id,
    avg(relationship_stability) as avg_relationship_stability,
    stddev_pop(story_yield) as yield_volatility
  from {{ ref('story_yield_curves') }}
  where creative_id is not null
  group by creative_id
),

belief_by_creative as (
  select
    crs.creative_id,
    avg(bac.belief_decay_rate) as avg_belief_decay_rate,
    avg(bac.belief_retention_index) as avg_belief_retention_index
  from {{ ref('creative_recruitment_signatures') }} crs
  inner join {{ ref('belief_attrition_curves') }} bac
    on crs.cohort_id = bac.cohort_id
  where crs.creative_id is not null
  group by crs.creative_id
),

refund_by_creative as (
  select
    crs.creative_id,
    count(distinct ar.refund_id) as refund_count,
    sum(coalesce(ar.total_refunded_amount, 0)) as total_refunded_amount
  from {{ ref('creative_recruitment_signatures') }} crs
  inner join {{ ref('dim_cohort') }} dc
    on crs.cohort_id = dc.cohort_id
  inner join {{ ref('analytics_orders') }} ao
    on dc.customer_id = ao.customer_id
  inner join {{ ref('analytics_refunds') }} ar
    on ao.order_id = ar.order_id
  where crs.creative_id is not null
  group by crs.creative_id
),

combined as (
  select
    c.creative_id,
    ns.expectation_violation_rate,
    ns.lived_experience_divergence,
    ns.narrative_concordance_score,
    sy.avg_relationship_stability,
    sy.yield_volatility,
    bb.avg_belief_decay_rate,
    bb.avg_belief_retention_index,
    rb.refund_count,
    rb.total_refunded_amount
  from creatives_from_narrative c
  left join narrative_structural ns on c.creative_id = ns.creative_id
  left join story_yield_agg sy on c.creative_id = sy.creative_id
  left join belief_by_creative bb on c.creative_id = bb.creative_id
  left join refund_by_creative rb on c.creative_id = rb.creative_id
),

with_signals as (
  select
    creative_id,
    expectation_violation_rate,
    lived_experience_divergence,
    narrative_concordance_score,
    avg_relationship_stability,
    yield_volatility,
    avg_belief_decay_rate,
    avg_belief_retention_index,
    refund_count,
    total_refunded_amount,
    greatest(0.0, coalesce(1.0 - narrative_concordance_score, 0)) as expectation_signal,
    greatest(0.0, least(1.0, coalesce(1.0 - avg_relationship_stability, 0) + coalesce(yield_volatility, 0) * 0.5)) as volatility_signal,
    greatest(0.0, least(1.0, coalesce(1.0 - avg_belief_retention_index, 0) + coalesce(avg_belief_decay_rate, 0))) as belief_signal,
    least(1.0, coalesce(refund_count, 0) / (1.0 + coalesce(refund_count, 0))) as refund_signal
  from combined
),

with_dominant as (
  select
    *,
    case
      when expectation_signal >= volatility_signal and expectation_signal >= refund_signal then 'expectation_inflation'
      when volatility_signal >= refund_signal then 'volatility_amplification'
      else 'misaligned_recruitment'
    end as dominant_regret_trigger
  from with_signals
)

select
  creative_id,
  dominant_regret_trigger,
  least(1.0, greatest(0.0,
    (expectation_signal + volatility_signal + belief_signal + refund_signal) / 4.0
  )) as regret_induction_probability,
  greatest(0.0, coalesce(expectation_violation_rate, 0)) as expectation_collapse_rate,
  greatest(0.0, coalesce(lived_experience_divergence, 0) + coalesce(avg_belief_decay_rate, 0)) as psychological_debt_index,
  least(1.0, greatest(0.0,
    (least(1.0, greatest(0.0, (expectation_signal + volatility_signal + belief_signal + refund_signal) / 4.0))
     + least(1.0, greatest(0.0, coalesce(expectation_violation_rate, 0)))
     + least(1.0, greatest(0.0, coalesce(lived_experience_divergence, 0) + coalesce(avg_belief_decay_rate, 0))))
    / 3.0
  )) as regret_risk_index
from with_dominant
