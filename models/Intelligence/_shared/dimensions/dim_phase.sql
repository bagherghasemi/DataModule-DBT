{{
  config(
    materialized='table',
    unique_key='phase_key'
  )
}}

/*
  Model: dim_phase
  Layer: Shared dimension (foundational ontology)
  Grain: One row per canonical structural phase
  Purpose: Fixed structural phase ontology for classification and joins
*/

select
  phase_key,
  phase_name,
  phase_order,
  phase_description
from (
  select 'extraction' as phase_key,
         'Extraction' as phase_name,
         1 as phase_order,
         'Short-term value capture with low relational stability' as phase_description
  union all
  select 'stability' as phase_key,
         'Stability' as phase_name,
         2 as phase_order,
         'Repeated, predictable value exchange with low volatility' as phase_description
  union all
  select 'illusion' as phase_key,
         'Illusion' as phase_name,
         3 as phase_order,
         'Apparent performance driven by fragile or misleading signals' as phase_description
  union all
  select 'hidden_compounding' as phase_key,
         'Hidden Compounding' as phase_name,
         4 as phase_order,
         'Slow, durable value accumulation with delayed visibility' as phase_description
)
order by phase_order