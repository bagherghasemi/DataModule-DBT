{{
  config(
    materialized='table',
    unique_key=['creative_id', 'date']
  )
}}

with creatives_from_narrative as (
  select distinct crs.creative_id
  from {{ ref('creative_recruitment_signatures') }} crs
  inner join {{ ref('narrative_concordance_scores') }} ncs
    on crs.creative_id = ncs.creative_id
  where crs.creative_id is not null
),

date_spine as (
  select date from {{ ref('dim_date') }}
),

creative_date_spine as (
  select
    c.creative_id,
    d.date
  from creatives_from_narrative c
  cross join date_spine d
),

recruited_cohorts_aligned as (
  select crs.creative_id, crs.cohort_id
  from {{ ref('creative_recruitment_signatures') }} crs
  inner join (select distinct cohort_id from {{ ref('identity_transition_matrix') }}) itm
    on crs.cohort_id = itm.cohort_id
),

creative_cohort_yield as (
  select
    rca.creative_id,
    ysc.date,
    ysc.expressed_desire,
    ysc.realized_value,
    ysc.relationship_stability
  from recruited_cohorts_aligned rca
  inner join {{ ref('yield_stability_curves') }} ysc
    on rca.cohort_id = ysc.cohort_id
),

creative_agg as (
  select
    creative_id,
    date,
    sum(expressed_desire) as expressed_desire,
    sum(realized_value) as realized_value,
    least(1.0, greatest(0.0, avg(relationship_stability))) as relationship_stability
  from creative_cohort_yield
  group by creative_id, date
),

spine_with_agg as (
  select
    cds.creative_id,
    cds.date,
    ca.expressed_desire,
    ca.realized_value,
    ca.relationship_stability
  from creative_date_spine cds
  left join creative_agg ca
    on cds.creative_id = ca.creative_id and cds.date = ca.date
),

with_carried_desire as (
  select
    creative_id,
    date,
    last_value(expressed_desire ignore nulls) over (
      partition by creative_id order by date
      rows between unbounded preceding and current row
    ) as expressed_desire,
    realized_value,
    relationship_stability
  from spine_with_agg
)

select
  creative_id,
  date,
  greatest(0.0, coalesce(expressed_desire, 0)) as expressed_desire,
  case when realized_value is not null then greatest(0.0, realized_value) else null end as realized_value,
  case when relationship_stability is not null then least(1.0, greatest(0.0, relationship_stability)) else null end as relationship_stability,
  case
    when coalesce(expressed_desire, 0) > 0 and realized_value is not null and relationship_stability is not null
      then (greatest(0.0, realized_value) * least(1.0, greatest(0.0, relationship_stability))) / expressed_desire
    when coalesce(expressed_desire, 0) = 0
      then 0.0
    else null
  end as story_yield
from with_carried_desire
