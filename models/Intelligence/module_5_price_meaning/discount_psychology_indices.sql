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

discount_exposure_by_cohort as (
  select
    dc.cohort_id,
    count(distinct ao.order_id) as orders_with_discount_exposure,
    count(distinct aoli.line_item_id) as line_items_with_discount
  from {{ ref('dim_cohort') }} dc
  left join {{ ref('analytics_orders') }} ao
    on ao.customer_id = dc.customer_id
  left join {{ ref('analytics_order_line_items') }} aoli
    on aoli.order_id = ao.order_id
  group by dc.cohort_id
),

recruitment_by_cohort as (
  select
    cohort_id,
    count(*) as creative_cohort_pairs
  from {{ ref('creative_recruitment_signatures') }}
  group by cohort_id
),

motivation_context as (
  select cohort_id
  from {{ ref('motivation_vectors') }}
),

belief_context as (
  select
    cohort_id,
    max(date) as latest_belief_date
  from {{ ref('belief_attrition_curves') }}
  group by cohort_id
),

base as (
  select
    ec.cohort_id
  from eligible_cohorts ec
  left join discount_exposure_by_cohort dec
    on ec.cohort_id = dec.cohort_id
  left join recruitment_by_cohort rbc
    on ec.cohort_id = rbc.cohort_id
  left join motivation_context mc
    on ec.cohort_id = mc.cohort_id
  left join belief_context bc
    on ec.cohort_id = bc.cohort_id
)

select
  base.cohort_id,
  cast(null as float64) as discount_trust_effect,
  cast(null as float64) as discount_dependency_risk,
  cast(null as float64) as discount_identity_threat,
  cast(null as float64) as discount_psychology_index
from base
