{{
  config(
    materialized='table',
    unique_key='population_stage'
  )
}}

/*
  Model: enum_population_stage
  Layer: Shared enum (ontology infrastructure)
  Grain: One row per canonical population stage enum value
  Purpose: Machine-stable population stage enum for joins and constraints
*/

select
  population_stage,
  population_stage_order
from (
  select 'exposure' as population_stage,
         1 as population_stage_order
  union all
  select 'engagement' as population_stage,
         2 as population_stage_order
  union all
  select 'purchase' as population_stage,
         3 as population_stage_order
  union all
  select 'loyalty' as population_stage,
         4 as population_stage_order
)
order by population_stage_order