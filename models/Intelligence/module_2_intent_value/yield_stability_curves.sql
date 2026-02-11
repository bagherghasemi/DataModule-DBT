{{
  config(
    materialized='table',
    unique_key=['cohort_id', 'date']
  )
}}

with eligible_cohorts as (
  select distinct itm.cohort_id
  from {{ ref('identity_transition_matrix') }} itm
  inner join {{ ref('desire_value_map') }} dvm
    on itm.cohort_id = dvm.cohort_id
  inner join {{ ref('reciprocity_differential_vectors') }} rdv
    on itm.cohort_id = rdv.cohort_id
),

date_spine as (
  select date from {{ ref('dim_date') }}
),

cohort_date_spine as (
  select
    ec.cohort_id,
    ds.date
  from eligible_cohorts ec
  cross join date_spine ds
),

cohort_members as (
  select customer_id, cohort_id
  from {{ ref('dim_cohort') }}
),

order_by_cohort_date as (
  select
    cm.cohort_id,
    date(ao.created_at) as date,
    sum(coalesce(ao.total_price, 0) - coalesce(ao.total_refunded, 0)) as order_net
  from cohort_members cm
  inner join {{ ref('analytics_orders') }} ao
    on ao.customer_id = cm.customer_id
  group by cm.cohort_id, date(ao.created_at)
),

refund_by_cohort_date as (
  select
    cm.cohort_id,
    date(ar.created_at) as date,
    sum(coalesce(ar.total_refunded_amount, 0)) as refund_amount
  from cohort_members cm
  inner join {{ ref('analytics_orders') }} ao
    on ao.customer_id = cm.customer_id
  inner join {{ ref('analytics_refunds') }} ar
    on ar.order_id = ao.order_id
  group by cm.cohort_id, date(ar.created_at)
),

realized_value_raw as (
  select
    cd.cohort_id,
    cd.date,
    coalesce(oc.order_net, 0) - coalesce(rc.refund_amount, 0) as raw_value
  from cohort_date_spine cd
  left join order_by_cohort_date oc
    on cd.cohort_id = oc.cohort_id and cd.date = oc.date
  left join refund_by_cohort_date rc
    on cd.cohort_id = rc.cohort_id and cd.date = rc.date
),

expressed_desire_align as (
  select
    cd.cohort_id,
    cd.date,
    greatest(0.0, dvm.desire_coordinate) as expressed_desire
  from cohort_date_spine cd
  inner join {{ ref('desire_value_map') }} dvm
    on cd.cohort_id = dvm.cohort_id
),

stability_align as (
  select
    bac.cohort_id,
    bac.date,
    least(1.0, greatest(0.0, coalesce(bac.stabilization_probability, bac.belief_retention_index, 0.0))) as relationship_stability
  from {{ ref('belief_attrition_curves') }} bac
  where bac.cohort_id in (select cohort_id from eligible_cohorts)
),

yield_inputs as (
  select
    cd.cohort_id,
    cd.date,
    ed.expressed_desire,
    greatest(0.0, coalesce(rv.raw_value, 0)) as realized_value,
    sa.relationship_stability
  from cohort_date_spine cd
  left join expressed_desire_align ed
    on cd.cohort_id = ed.cohort_id and cd.date = ed.date
  left join realized_value_raw rv
    on cd.cohort_id = rv.cohort_id and cd.date = rv.date
  left join stability_align sa
    on cd.cohort_id = sa.cohort_id and cd.date = sa.date
)

select
  cohort_id,
  date,
  expressed_desire,
  realized_value,
  relationship_stability,
  case
    when coalesce(expressed_desire, 0) > 0
      then (realized_value * relationship_stability) / expressed_desire
    else 0.0
  end as human_yield
from yield_inputs
