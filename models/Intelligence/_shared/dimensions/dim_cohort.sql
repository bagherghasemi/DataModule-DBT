{{
  config(
    materialized='table',
    unique_key='customer_id'
  )
}}

/*
  Model: dim_cohort

  Layer: Shared dimension (foundational ontology)

  Grain: One row per customer_id (canonical cohort membership)
*/

with first_completed_order_per_customer as (
  select
    ao.customer_id,
    ao.order_id,
    ao.created_at as first_order_timestamp,
    date(ao.created_at) as first_order_date,
    date_trunc(date(ao.created_at), month) as first_purchase_month,
    ao.total_price as first_order_total_price,
    ao.billing_address_country as billing_country,
    ao.billing_address_region as billing_region,
    ao.billing_address_city as billing_city,
    row_number() over (
      partition by ao.customer_id
      order by ao.created_at
    ) as order_rank
  from {{ ref('analytics_orders') }} ao
  where ao.customer_id is not null
),

first_order_customers as (
  select
    customer_id,
    order_id,
    first_order_timestamp,
    first_order_date,
    first_purchase_month,
    first_order_total_price,
    billing_country,
    billing_region,
    billing_city
  from first_completed_order_per_customer
  where order_rank = 1
),

first_order_product_category as (
  select
    aoli.order_id,
    any_value(ap.product_type) as first_product_category
  from {{ ref('analytics_order_line_items') }} aoli
  left join {{ ref('analytics_products') }} ap
    on aoli.product_id = ap.product_id
  group by
    aoli.order_id
),

cohort_source as (
  select
    foc.customer_id,
    foc.first_order_timestamp,
    foc.first_order_date,
    foc.first_purchase_month,
    foc.first_order_total_price,
    foc.billing_country,
    foc.billing_region,
    foc.billing_city,
    fopc.first_product_category
  from first_order_customers foc
  left join first_order_product_category fopc
    on foc.order_id = fopc.order_id
),

cohort_with_id as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'first_product_category',
      'billing_country',
      'billing_region',
      'billing_city',
      'first_purchase_month'
    ]) }} as cohort_id,
    customer_id,
    first_order_timestamp,
    first_order_date,
    first_purchase_month,
    first_order_total_price,
    billing_country,
    billing_region,
    billing_city,
    first_product_category
  from cohort_source
)

select
  customer_id,
  cohort_id,
  concat_ws(' / ',
    coalesce(first_product_category, 'unknown_product_category'),
    coalesce(billing_country, 'unknown_country'),
    coalesce(cast(first_purchase_month as string), 'unknown_month')
  ) as cohort_name,
  first_order_timestamp as cohort_created_at,
  first_product_category,
  first_order_total_price,
  billing_country,
  billing_region,
  billing_city,
  first_purchase_month
from cohort_with_id