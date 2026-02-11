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

desire_value_context as (
  select
    cohort_id,
    desire_coordinate,
    value_coordinate,
    structural_zone_label
  from {{ ref('desire_value_map') }}
),

recruitment_by_cohort as (
  select
    cohort_id,
    count(*) as creative_cohort_pairs,
    avg(recruitment_probability) as avg_recruitment_probability,
    avg(recruitment_strength) as avg_recruitment_strength,
    avg(recruitment_volatility) as avg_recruitment_volatility
  from {{ ref('creative_recruitment_signatures') }}
  group by cohort_id
),

orders_by_cohort as (
  select
    dc.cohort_id,
    count(distinct ao.order_id) as order_count
  from {{ ref('dim_cohort') }} dc
  inner join {{ ref('analytics_orders') }} ao
    on ao.customer_id = dc.customer_id
  group by dc.cohort_id
),

refunds_by_cohort as (
  select
    dc.cohort_id,
    count(distinct ar.refund_id) as refund_count,
    sum(ar.total_refunded_amount) as total_refunded
  from {{ ref('analytics_refunds') }} ar
  inner join {{ ref('analytics_orders') }} ao
    on ar.order_id = ao.order_id
  inner join {{ ref('dim_cohort') }} dc
    on ao.customer_id = dc.customer_id
  group by dc.cohort_id
),

base as (
  select
    ec.cohort_id
  from eligible_cohorts ec
  left join identity_context ic
    on ec.cohort_id = ic.cohort_id
  left join desire_value_context dvc
    on ec.cohort_id = dvc.cohort_id
  left join recruitment_by_cohort rbc
    on ec.cohort_id = rbc.cohort_id
  left join orders_by_cohort obc
    on ec.cohort_id = obc.cohort_id
  left join refunds_by_cohort rfbc
    on ec.cohort_id = rfbc.cohort_id
)

select
  base.cohort_id,

  cast(null as numeric) as motivation_status_intensity,
  cast(null as numeric) as motivation_safety_intensity,
  cast(null as numeric) as motivation_control_intensity,
  cast(null as numeric) as motivation_belonging_intensity,
  cast(null as numeric) as motivation_relief_intensity,
  cast(null as numeric) as motivation_meaning_intensity,
  cast(null as numeric) as motivation_pleasure_intensity,
  cast(null as numeric) as motivation_certainty_intensity,

  cast(null as numeric) as motivation_coherence,
  cast(null as numeric) as motivation_conflict_index,
  cast(null as numeric) as motivational_entropy,

  struct(
    cast(null as numeric) as motivation_status_intensity,
    cast(null as numeric) as motivation_safety_intensity,
    cast(null as numeric) as motivation_control_intensity,
    cast(null as numeric) as motivation_belonging_intensity,
    cast(null as numeric) as motivation_relief_intensity,
    cast(null as numeric) as motivation_meaning_intensity,
    cast(null as numeric) as motivation_pleasure_intensity,
    cast(null as numeric) as motivation_certainty_intensity
  ) as motivation_vector

from base
