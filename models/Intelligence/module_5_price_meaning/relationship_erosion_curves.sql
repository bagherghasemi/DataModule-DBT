{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'discount_intensity_band']
  )
}}

with cohorts as (
  select distinct cohort_id
  from {{ ref('belief_attrition_curves') }}
),

bands as (
  select discount_intensity_band_key as discount_intensity_band
  from {{ ref('dim_discount_intensity_band') }}
),

spine as (
  select
    c.cohort_id,
    b.discount_intensity_band
  from cohorts c
  cross join bands b
),

order_by_cohort as (
  select
    dc.cohort_id,
    count(distinct ao.order_id) as order_count
  from {{ ref('dim_cohort') }} dc
  left join {{ ref('analytics_orders') }} ao
    on ao.customer_id = dc.customer_id
  group by dc.cohort_id
),

refund_by_cohort as (
  select
    dc.cohort_id,
    count(distinct ar.refund_id) as refund_count
  from {{ ref('dim_cohort') }} dc
  left join {{ ref('analytics_orders') }} ao
    on ao.customer_id = dc.customer_id
  left join {{ ref('analytics_refunds') }} ar
    on ar.order_id = ao.order_id
  group by dc.cohort_id
),

belief_by_cohort as (
  select
    cohort_id,
    avg(belief_decay_rate) as belief_decay_rate,
    avg(stabilization_probability) as stabilization_probability
  from {{ ref('belief_attrition_curves') }}
  group by cohort_id
),

payoff_by_cohort as (
  select
    cohort_id,
    avg(emotional_hangover_level) as emotional_hangover_level
  from {{ ref('emotional_payoff_curves') }}
  group by cohort_id
),

base as (
  select
    s.cohort_id,
    s.discount_intensity_band,
    obc.order_count,
    rbc.refund_count,
    bac.belief_decay_rate,
    bac.stabilization_probability,
    pbc.emotional_hangover_level
  from spine s
  left join order_by_cohort obc on s.cohort_id = obc.cohort_id
  left join refund_by_cohort rbc on s.cohort_id = rbc.cohort_id
  left join belief_by_cohort bac on s.cohort_id = bac.cohort_id
  left join payoff_by_cohort pbc on s.cohort_id = pbc.cohort_id
)

select
  cohort_id,
  discount_intensity_band,
  cast(null as float64) as trust_erosion_probability,
  cast(null as float64) as attachment_decay_rate,
  cast(null as float64) as identity_disruption_index,
  cast(null as float64) as relationship_erosion_threshold
from base
