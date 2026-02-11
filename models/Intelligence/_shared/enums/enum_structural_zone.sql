{{
  config(
    materialized='table',
    unique_key='structural_zone'
  )
}}

/*
  Model: enum_structural_zone
  Layer: Shared enum (ontology infrastructure)
  Grain: One row per canonical structural zone enum value
  Purpose: Machine-stable structural zone enum for joins and constraints
*/

select
  structural_zone,
  structural_zone_order
from (
  select 'extraction' as structural_zone,
         1 as structural_zone_order
  union all
  select 'stability' as structural_zone,
         2 as structural_zone_order
  union all
  select 'illusion' as structural_zone,
         3 as structural_zone_order
  union all
  select 'hidden_compounding' as structural_zone,
         4 as structural_zone_order
)
order by structural_zone_order
/*
  This file defines the `enum_structural_zone` model, which is a canonical enumeration of structural zones