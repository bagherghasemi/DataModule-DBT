{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'population_stage']
  )
}}

/*
  Model: identity_population_profiles
  Module: Module 1 — Identity & Reality Gap
  Grain: One row per cohort × population_stage identity profile
  Purpose: Canonical identity composition for each cohort at each population stage
*/

with cohort_population_stage_space as (
  select
    c.cohort_id,
    eps.population_stage
  from {{ ref('dim_cohort') }} c
  cross join {{ ref('enum_population_stage') }} eps
),

cohort_members as (
  select
    customer_id,
    cohort_id
  from {{ ref('dim_cohort') }}
),

customer_order_counts as (
  select
    cm.cohort_id,
    cm.customer_id,
    count(*) as order_count
  from cohort_members cm
  inner join {{ ref('analytics_orders') }} ao
    on ao.customer_id = cm.customer_id
  group by
    cm.cohort_id,
    cm.customer_id
),

purchase_stage_population as (
  select
    cohort_id,
    'purchase' as population_stage,
    count(distinct customer_id) as population_size
  from customer_order_counts
  where order_count >= 1
  group by
    cohort_id
),

loyalty_stage_population as (
  select
    cohort_id,
    'loyalty' as population_stage,
    count(distinct customer_id) as population_size
  from customer_order_counts
  where order_count >= 2
  group by
    cohort_id
),

exposure_stage_population as (
  select
    distinct cohort_id,
    'exposure' as population_stage,
    0 as population_size
  from {{ ref('dim_cohort') }}
),

engagement_stage_population as (
  select
    distinct cohort_id,
    'engagement' as population_stage,
    0 as population_size
  from {{ ref('dim_cohort') }}
),

stage_population as (
  select * from purchase_stage_population
  union all
  select * from loyalty_stage_population
  union all
  select * from exposure_stage_population
  union all
  select * from engagement_stage_population
)

select
  -- Grain keys
  cps.cohort_id,
  cps.population_stage,

  -- Core measures
  coalesce(sp.population_size, 0) as population_size,
  case
    when sum(coalesce(sp.population_size, 0)) over (partition by cps.cohort_id) = 0
      then null
    else coalesce(sp.population_size, 0)
      / sum(coalesce(sp.population_size, 0)) over (partition by cps.cohort_id)
  end as normalized_presence_weight,
  cast(null as numeric) as identity_entropy,
  cast(null as numeric) as behavioral_coherence,

  -- Core identity dimensions (scalar numeric expressions)
  cast(null as numeric) as aspirational_score,
  cast(null as numeric) as control_orientation_score,
  cast(null as numeric) as novelty_orientation_score,
  cast(null as numeric) as price_sensitivity_score,
  cast(null as numeric) as stability_orientation_score,
  cast(null as numeric) as belonging_orientation_score

from cohort_population_stage_space cps
left join stage_population sp
  on cps.cohort_id = sp.cohort_id
 and cps.population_stage = sp.population_stage
