{{
  config(
    materialized='table',
    unique_key='population_stage_key'
  )
}}

/*
  Model: dim_population_stage
  Layer: Shared dimension (foundational ontology)
  Grain: One row per canonical population stage
  Purpose: Fixed population-stage ontology for structural joins
*/

select
  population_stage_key,
  population_stage_name,
  population_stage_order,
  population_stage_description
from (
  select 'exposure' as population_stage_key,
         'exposure' as population_stage_name,
         1 as population_stage_order,
         'Humans who were exposed to the brand or message' as population_stage_description
  union all
  select 'engagement' as population_stage_key,
         'engagement' as population_stage_name,
         2 as population_stage_order,
         'Humans who expressed voluntary attention or interaction' as population_stage_description
  union all
  select 'purchase' as population_stage_key,
         'purchase' as population_stage_name,
         3 as population_stage_order,
         'Humans who committed economically' as population_stage_description
  union all
  select 'loyalty' as population_stage_key,
         'loyalty' as population_stage_name,
         4 as population_stage_order,
         'Humans who demonstrated repeat or attachment behavior' as population_stage_description
)
order by population_stage_order