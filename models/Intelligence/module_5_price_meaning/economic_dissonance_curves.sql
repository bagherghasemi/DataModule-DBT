{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'date']
  )
}}

with spine as (
  select
    epc.cohort_id,
    epc.date
  from {{ ref('emotional_payoff_curves') }} epc
  inner join {{ ref('dim_date') }} dd
    on dd.date = epc.date
),

pmv as (
  select
    cohort_id,
    price_meaning_coherence
  from {{ ref('price_meaning_vectors') }}
),

epc as (
  select
    cohort_id,
    date,
    emotional_satisfaction_level
  from {{ ref('emotional_payoff_curves') }}
),

cohort_members as (
  select customer_id, cohort_id
  from {{ ref('dim_cohort') }}
),

refund_by_cohort_date as (
  select
    cm.cohort_id,
    date(ar.created_at) as date,
    sum(coalesce(ar.total_refunded_amount, 0)) as refund_amount,
    count(distinct ar.refund_id) as refund_count
  from cohort_members cm
  inner join {{ ref('analytics_orders') }} ao
    on ao.customer_id = cm.customer_id
  inner join {{ ref('analytics_refunds') }} ar
    on ar.order_id = ao.order_id
  group by cm.cohort_id, date(ar.created_at)
),

base as (
  select
    s.cohort_id,
    s.date,
    pmv.price_meaning_coherence as anticipated_value,
    epc.emotional_satisfaction_level as experienced_value
  from spine s
  left join pmv on s.cohort_id = pmv.cohort_id
  left join epc on s.cohort_id = epc.cohort_id and s.date = epc.date
  left join refund_by_cohort_date rbc on s.cohort_id = rbc.cohort_id and s.date = rbc.date
)

select
  cohort_id,
  date,
  case
    when anticipated_value is not null
    then least(1.0, greatest(0.0, anticipated_value))
    else null
  end as anticipated_value,
  case
    when experienced_value is not null
    then least(1.0, greatest(0.0, experienced_value))
    else null
  end as experienced_value,
  cast(null as float64) as reflective_value,
  case
    when anticipated_value is not null and experienced_value is not null
    then greatest(0.0, abs(
      least(1.0, greatest(0.0, anticipated_value)) - least(1.0, greatest(0.0, experienced_value))
    ))
    else null
  end as dissonance_magnitude
from base
