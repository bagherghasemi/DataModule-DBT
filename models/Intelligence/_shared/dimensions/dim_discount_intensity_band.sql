{{
  config(
    materialized='table',
    unique_key='discount_intensity_band_key'
  )
}}

/*
  Model: dim_discount_intensity_band
  Layer: Shared dimension (foundational normalization ontology)
  Grain: One row per canonical discount intensity band
  Purpose: Fixed discount intensity normalization axis for consistent interpretation
*/

select
  discount_intensity_band_key,
  discount_intensity_band_name,
  discount_intensity_band_order,
  discount_intensity_band_description
from (
  select 'no_discount' as discount_intensity_band_key,
         'No Discount' as discount_intensity_band_name,
         1 as discount_intensity_band_order,
         'No price reduction applied' as discount_intensity_band_description
  union all
  select 'light_discount' as discount_intensity_band_key,
         'Light Discount' as discount_intensity_band_name,
         2 as discount_intensity_band_order,
         'Small incentive that does not materially alter perceived value' as discount_intensity_band_description
  union all
  select 'moderate_discount' as discount_intensity_band_key,
         'Moderate Discount' as discount_intensity_band_name,
         3 as discount_intensity_band_order,
         'Noticeable incentive that influences decision timing' as discount_intensity_band_description
  union all
  select 'heavy_discount' as discount_intensity_band_key,
         'Heavy Discount' as discount_intensity_band_name,
         4 as discount_intensity_band_order,
         'Strong incentive that alters value perception' as discount_intensity_band_description
  union all
  select 'extreme_discount' as discount_intensity_band_key,
         'Extreme Discount' as discount_intensity_band_name,
         5 as discount_intensity_band_order,
         'Highly aggressive incentive that risks value erosion' as discount_intensity_band_description
)
order by discount_intensity_band_order