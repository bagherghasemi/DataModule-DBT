{{
  config(
    materialized='table',
    unique_key='source_id'
  )
}}

with meta_delivery as (

  select distinct
    'meta_paid' as source_channel,
    cast(null as string) as source_audience_type,
    cast(null as string) as source_creative_cluster,
    cast(null as string) as source_intent_frame
  from {{ ref('analytics_campaigns') }}

),

with_id as (

  select
    {{ dbt_utils.generate_surrogate_key([
      'source_channel',
      'source_audience_type',
      'source_creative_cluster',
      'source_intent_frame'
    ]) }} as source_id,
    source_channel,
    source_audience_type,
    source_creative_cluster,
    source_intent_frame,
    concat_ws(' | ',
      source_channel,
      coalesce(source_audience_type, '—'),
      coalesce(source_creative_cluster, '—'),
      coalesce(source_intent_frame, '—')
    ) as acquisition_source_identity
  from meta_delivery

)

select
  source_id,
  source_channel,
  source_audience_type,
  source_creative_cluster,
  source_intent_frame,
  acquisition_source_identity
from with_id
