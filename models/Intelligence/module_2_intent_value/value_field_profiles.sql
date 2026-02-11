{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with eligible_cohorts as (
  select distinct
    cohort_id
  from {{ ref('dim_cohort') }}
),

cohort_members as (
  select
    customer_id,
    cohort_id
  from {{ ref('dim_cohort') }}
),

cohort_orders as (
  select
    cm.cohort_id,
    ao.order_id,
    ao.customer_id,
    ao.created_at,
    ao.total_price,
    ao.total_shipping_price,
    ao.total_discounts,
    ao.total_tax,
    ao.total_refunded,
    ao.cancelled_at,
    ao.financial_status,
    ao.fulfillment_status
  from cohort_members cm
  inner join {{ ref('analytics_orders') }} ao
    on ao.customer_id = cm.customer_id
),

cohort_refunds as (
  select
    co.cohort_id,
    ar.refund_id,
    ar.order_id,
    ar.total_refunded_amount,
    ar.created_at as refund_created_at
  from cohort_orders co
  inner join {{ ref('analytics_refunds') }} ar
    on ar.order_id = co.order_id
),

cohort_order_line_items as (
  select
    co.cohort_id,
    aoli.order_id,
    aoli.product_id,
    aoli.quantity,
    aoli.price,
    aoli.total_discount,
    aoli.fulfillment_status,
    aoli.requires_shipping,
    aoli.taxable
  from cohort_orders co
  inner join {{ ref('analytics_order_line_items') }} aoli
    on aoli.order_id = co.order_id
),

cohort_product_context as (
  select
    coli.cohort_id,
    coli.order_id,
    coli.product_id,
    ap.product_type,
    ap.vendor
  from cohort_order_line_items coli
  left join {{ ref('analytics_products') }} ap
    on coli.product_id = ap.product_id
),

transactional_signals as (
  select
    ec.cohort_id,
    count(distinct co.order_id) as total_orders,
    count(distinct co.customer_id) as total_customers,
    sum(co.total_price) as total_revenue,
    sum(co.total_shipping_price) as total_shipping_costs,
    sum(co.total_discounts) as total_discounts_applied,
    sum(co.total_tax) as total_tax_amount,
    sum(co.total_refunded) as total_refunded_amount,
    count(distinct case when co.cancelled_at is not null then co.order_id end) as cancelled_orders,
    count(distinct case when co.fulfillment_status != 'fulfilled' then co.order_id end) as unfulfilled_orders,
    count(distinct cr.refund_id) as refund_count,
    sum(cr.total_refunded_amount) as total_refund_amount,
    count(distinct coli.product_id) as distinct_products,
    count(distinct coli.order_id) as orders_with_line_items,
    sum(coli.quantity) as total_quantity,
    count(distinct case when coli.requires_shipping = true then coli.order_id end) as orders_requiring_shipping,
    count(distinct case when coli.taxable = true then coli.order_id end) as orders_with_tax
  from eligible_cohorts ec
  left join cohort_orders co
    on ec.cohort_id = co.cohort_id
  left join cohort_refunds cr
    on ec.cohort_id = cr.cohort_id
  left join cohort_order_line_items coli
    on ec.cohort_id = coli.cohort_id
  group by
    ec.cohort_id
),

identity_context as (
  select
    cohort_id,
    sum(population_size) as total_population
  from {{ ref('identity_population_profiles') }}
  group by cohort_id
),

belief_context as (
  select
    cohort_id,
    avg(belief_retention_index) as avg_belief_retention_index,
    avg(belief_decay_rate) as avg_belief_decay_rate
  from {{ ref('belief_attrition_curves') }}
  where belief_retention_index is not null
  group by cohort_id
),

cohort_value_base as (
  select
    ts.cohort_id,
    coalesce(ic.total_population, 0) as total_population,
    coalesce(bc.avg_belief_retention_index, 0) as avg_belief_retention_index,
    coalesce(bc.avg_belief_decay_rate, 0) as avg_belief_decay_rate,
    coalesce(ts.total_orders, 0) as total_orders,
    coalesce(ts.total_customers, 0) as total_customers,
    coalesce(ts.total_revenue, 0) as total_revenue,
    coalesce(ts.total_shipping_costs, 0) as total_shipping_costs,
    coalesce(ts.total_discounts_applied, 0) as total_discounts_applied,
    coalesce(ts.total_tax_amount, 0) as total_tax_amount,
    coalesce(ts.total_refunded_amount, 0) as total_refunded_amount,
    coalesce(ts.cancelled_orders, 0) as cancelled_orders,
    coalesce(ts.unfulfilled_orders, 0) as unfulfilled_orders,
    coalesce(ts.refund_count, 0) as refund_count,
    coalesce(ts.total_refund_amount, 0) as total_refund_amount,
    coalesce(ts.distinct_products, 0) as distinct_products,
    coalesce(ts.total_quantity, 0) as total_quantity,
    coalesce(ts.orders_requiring_shipping, 0) as orders_requiring_shipping,
    coalesce(ts.orders_with_tax, 0) as orders_with_tax
  from transactional_signals ts
  left join identity_context ic
    on ts.cohort_id = ic.cohort_id
  left join belief_context bc
    on ts.cohort_id = bc.cohort_id
),

value_dimensions_raw as (
  select
    cohort_id,
    case
      when total_orders > 0
        then (
          (coalesce(total_shipping_costs, 0) / nullif(total_revenue, 0)) * 0.4 +
          (coalesce(total_discounts_applied, 0) / nullif(total_revenue, 0)) * 0.3 +
          (coalesce(cancelled_orders, 0) / nullif(total_orders, 0)) * 0.2 +
          (coalesce(unfulfilled_orders, 0) / nullif(total_orders, 0)) * 0.1
        )
      else 0.0
    end as friction_load_raw,
    case
      when total_orders > 0
        then (
          (coalesce(refund_count, 0) / nullif(total_orders, 0)) * 0.5 +
          (coalesce(total_refund_amount, 0) / nullif(total_revenue, 0)) * 0.5
        )
      else 0.0
    end as regret_load_raw,
    case
      when total_customers > 0
        then (
          (coalesce(total_orders, 0) / nullif(total_customers, 0)) * 0.3 +
          (coalesce(distinct_products, 0) / nullif(total_orders, 0)) * 0.2 +
          (coalesce(orders_requiring_shipping, 0) / nullif(total_orders, 0)) * 0.3 +
          (coalesce(orders_with_tax, 0) / nullif(total_orders, 0)) * 0.2
        ) / 10.0
      else 0.0
    end as support_cost_load_raw
  from cohort_value_base
),

value_dimensions_normalized as (
  select
    vdr.cohort_id,
    least(1.0, greatest(0.0, vdr.friction_load_raw)) as friction_load,
    least(1.0, greatest(0.0, vdr.regret_load_raw)) as regret_load,
    least(1.0, greatest(0.0, vdr.support_cost_load_raw)) as support_cost_load
  from value_dimensions_raw vdr
),

realized_value_calc as (
  select
    vdn.cohort_id,
    vdn.friction_load,
    vdn.regret_load,
    vdn.support_cost_load,
    greatest(0.0,
      case
        when cvb.total_revenue > 0
          then (cvb.total_revenue - cvb.total_refunded_amount) / nullif(cvb.total_revenue, 0) -
               (vdn.friction_load + vdn.regret_load + vdn.support_cost_load) / 3.0
        else 0.0
      end
    ) as realized_value
  from value_dimensions_normalized vdn
  left join cohort_value_base cvb
    on vdn.cohort_id = cvb.cohort_id
),

value_coherence_calc as (
  select
    rvc.cohort_id,
    rvc.friction_load,
    rvc.regret_load,
    rvc.support_cost_load,
    rvc.realized_value,
    case
      when rvc.friction_load + rvc.regret_load + rvc.support_cost_load > 0
        then 1.0 - (
          abs(rvc.friction_load - rvc.regret_load) +
          abs(rvc.regret_load - rvc.support_cost_load) +
          abs(rvc.support_cost_load - rvc.friction_load)
        ) / 3.0
      else 0.0
    end as value_coherence
  from realized_value_calc rvc
),

order_refund_aggregated as (
  select
    order_id,
    sum(total_refunded_amount) as total_refunded_amount,
    count(distinct refund_id) as refund_count
  from cohort_refunds
  group by order_id
),

order_level_value_signals as (
  select
    co.cohort_id,
    co.order_id,
    co.total_price as order_revenue,
    coalesce(ora.total_refunded_amount, 0) as order_refund_amount,
    case
      when co.total_price > 0
        then (
          (coalesce(co.total_shipping_price, 0) / co.total_price) * 0.4 +
          (coalesce(co.total_discounts, 0) / co.total_price) * 0.3 +
          (case when co.cancelled_at is not null then 1.0 else 0.0 end) * 0.2 +
          (case when co.fulfillment_status != 'fulfilled' then 1.0 else 0.0 end) * 0.1
        )
      else 0.0
    end as order_friction_load,
    case
      when co.total_price > 0
        then (
          (case when ora.refund_count > 0 then 1.0 else 0.0 end) * 0.5 +
          (coalesce(ora.total_refunded_amount, 0) / co.total_price) * 0.5
        )
      else 0.0
    end as order_regret_load
  from cohort_orders co
  left join order_refund_aggregated ora
    on co.order_id = ora.order_id
),

cohort_order_volatility as (
  select
    cohort_id,
    case
      when count(*) > 1 and avg(order_revenue) > 0
        then stddev(order_revenue) / nullif(avg(order_revenue), 0)
      else 0.0
    end as revenue_coefficient_of_variation,
    case
      when count(*) > 1
        then stddev(order_friction_load)
      else 0.0
    end as friction_load_stddev,
    case
      when count(*) > 1
        then stddev(order_regret_load)
      else 0.0
    end as regret_load_stddev
  from order_level_value_signals
  group by cohort_id
),

value_volatility_calc as (
  select
    vcc.cohort_id,
    vcc.friction_load,
    vcc.regret_load,
    vcc.support_cost_load,
    vcc.realized_value,
    vcc.value_coherence,
    coalesce(
      (cov.revenue_coefficient_of_variation * 0.4 +
       cov.friction_load_stddev * 0.3 +
       cov.regret_load_stddev * 0.3),
      0.0
    ) as value_volatility
  from value_coherence_calc vcc
  left join cohort_order_volatility cov
    on vcc.cohort_id = cov.cohort_id
)

select
  cohort_id,
  friction_load,
  regret_load,
  support_cost_load,
  realized_value,
  value_coherence,
  value_volatility
from value_volatility_calc
