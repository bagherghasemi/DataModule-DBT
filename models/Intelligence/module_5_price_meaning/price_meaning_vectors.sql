{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with eligible_cohorts as (
  select distinct cohort_id
  from {{ ref('dim_cohort') }}
),

identity_context as (
  select
    cohort_id,
    sum(population_size) as total_population
  from {{ ref('identity_population_profiles') }}
  group by cohort_id
),

motivation_context as (
  select cohort_id
  from {{ ref('motivation_vectors') }}
),

price_exposure_by_cohort as (
  select
    dc.cohort_id,
    count(distinct ao.order_id) as order_count,
    count(distinct aoli.line_item_id) as line_item_count
  from {{ ref('dim_cohort') }} dc
  left join {{ ref('analytics_orders') }} ao
    on ao.customer_id = dc.customer_id
  left join {{ ref('analytics_order_line_items') }} aoli
    on aoli.order_id = ao.order_id
  group by dc.cohort_id
),

base as (
  select ec.cohort_id
  from eligible_cohorts ec
  left join identity_context ic on ec.cohort_id = ic.cohort_id
  left join motivation_context mc on ec.cohort_id = mc.cohort_id
  left join price_exposure_by_cohort pec on ec.cohort_id = pec.cohort_id
)

select
  base.cohort_id,

  cast(null as numeric) as price_meaning_smart_choice_intensity,
  cast(null as numeric) as price_meaning_investing_in_self_intensity,
  cast(null as numeric) as price_meaning_exploitation_intensity,
  cast(null as numeric) as price_meaning_risk_intensity,
  cast(null as numeric) as price_meaning_safety_intensity,
  cast(null as numeric) as price_meaning_treat_intensity,
  cast(null as numeric) as price_meaning_identity_intensity,

  cast(null as numeric) as price_meaning_coherence,
  cast(null as numeric) as price_meaning_volatility,

  struct(
    cast(null as numeric) as price_meaning_smart_choice_intensity,
    cast(null as numeric) as price_meaning_investing_in_self_intensity,
    cast(null as numeric) as price_meaning_exploitation_intensity,
    cast(null as numeric) as price_meaning_risk_intensity,
    cast(null as numeric) as price_meaning_safety_intensity,
    cast(null as numeric) as price_meaning_treat_intensity,
    cast(null as numeric) as price_meaning_identity_intensity
  ) as price_meaning_vector,

  cast(null as numeric) as identity_anchoring_score

from base
