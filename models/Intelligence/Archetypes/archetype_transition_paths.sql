{{
  config(
    materialized='table',
    unique_key=['from_archetype_id', 'to_archetype_id', 'date']
  )
}}

with valid_dates as (
  select d.date
  from {{ ref('dim_date') }} d
  inner join {{ ref('dim_date') }} prev
    on prev.date = date_sub(d.date, interval 1 day)
),

archetype_pairs as (
  select
    a_from.archetype_id as from_archetype_id,
    a_to.archetype_id as to_archetype_id
  from {{ ref('archetype_definitions') }} a_from
  cross join {{ ref('archetype_definitions') }} a_to
  where a_from.archetype_id != a_to.archetype_id
),

spine as (
  select
    ap.from_archetype_id,
    ap.to_archetype_id,
    vd.date
  from archetype_pairs ap
  cross join valid_dates vd
),

curr as (
  select
    cohort_id,
    archetype_id,
    date,
    membership_weight
  from {{ ref('cohort_to_archetype_mapping') }}
),

prev as (
  select
    cohort_id,
    archetype_id,
    date_sub(date, interval 1 day) as date,
    membership_weight as prev_weight
  from {{ ref('cohort_to_archetype_mapping') }}
),

delta_by_cohort as (
  select
    c.cohort_id,
    c.date,
    c.archetype_id,
    c.membership_weight as curr_weight,
    coalesce(p.prev_weight, 0) as prev_weight,
    c.membership_weight - coalesce(p.prev_weight, 0) as delta
  from curr c
  left join prev p
    on c.cohort_id = p.cohort_id
    and c.archetype_id = p.archetype_id
    and c.date = p.date
  inner join valid_dates vd on c.date = vd.date
),

cohort_totals as (
  select
    cohort_id,
    date,
    sum(greatest(0, delta)) as total_increase
  from delta_by_cohort
  group by cohort_id, date
  having sum(greatest(0, delta)) > 0
),

cohort_flows as (
  select
    d.cohort_id,
    d.date,
    d.archetype_id as from_archetype_id,
    i.archetype_id as to_archetype_id,
    greatest(0, -d.delta) * (greatest(0, i.delta) / ct.total_increase) as flow
  from delta_by_cohort d
  inner join delta_by_cohort i
    on d.cohort_id = i.cohort_id
    and d.date = i.date
    and d.archetype_id != i.archetype_id
    and d.delta < 0
    and i.delta > 0
  inner join cohort_totals ct
    on d.cohort_id = ct.cohort_id
    and d.date = ct.date
),

raw_flows as (
  select
    from_archetype_id,
    to_archetype_id,
    date,
    sum(flow) as transition_weight
  from cohort_flows
  group by from_archetype_id, to_archetype_id, date
),

outgoing_totals as (
  select
    from_archetype_id,
    date,
    sum(transition_weight) as total_outgoing
  from raw_flows
  group by from_archetype_id, date
),

flows_with_prob as (
  select
    rf.from_archetype_id,
    rf.to_archetype_id,
    rf.date,
    rf.transition_weight,
    case
      when coalesce(ot.total_outgoing, 0) = 0 then 0
      else least(1.0, rf.transition_weight / ot.total_outgoing)
    end as transition_probability
  from raw_flows rf
  left join outgoing_totals ot
    on rf.from_archetype_id = ot.from_archetype_id
    and rf.date = ot.date
),

reverse_flows as (
  select
    to_archetype_id as from_archetype_id,
    from_archetype_id as to_archetype_id,
    date,
    transition_weight as reverse_weight
  from raw_flows
),

flows_with_net as (
  select
    f.from_archetype_id,
    f.to_archetype_id,
    f.date,
    f.transition_weight,
    f.transition_probability,
    f.transition_weight - coalesce(rf.reverse_weight, 0) as net_flow
  from flows_with_prob f
  left join reverse_flows rf
    on f.from_archetype_id = rf.from_archetype_id
    and f.to_archetype_id = rf.to_archetype_id
    and f.date = rf.date
),

conf_from as (
  select archetype_id, date, confidence_score
  from {{ ref('archetype_confidence_scores') }}
),

conf_to as (
  select archetype_id, date, confidence_score
  from {{ ref('archetype_confidence_scores') }}
),

spine_with_conf as (
  select
    s.from_archetype_id,
    s.to_archetype_id,
    s.date,
    coalesce(cf.confidence_score, 0) as conf_from,
    coalesce(ct.confidence_score, 0) as conf_to
  from spine s
  left join conf_from cf
    on s.from_archetype_id = cf.archetype_id
    and s.date = cf.date
  left join conf_to ct
    on s.to_archetype_id = ct.archetype_id
    and s.date = ct.date
)

select
  swc.from_archetype_id,
  swc.to_archetype_id,
  swc.date,
  coalesce(f.transition_weight, 0) as transition_weight,
  coalesce(f.transition_probability, 0) as transition_probability,
  case
    when coalesce(f.net_flow, 0) > 0 then 'outward'
    when coalesce(f.net_flow, 0) < 0 then 'inward'
    else 'neutral'
  end as net_flow_direction,
  case
    when coalesce(f.transition_weight, 0) = 0 then 'stable'
    else 'drift'
  end as transition_stability,
  coalesce(f.transition_weight, 0)
    * least(swc.conf_from, swc.conf_to) as confidence_adjusted_flow
from spine_with_conf swc
left join flows_with_net f
  on swc.from_archetype_id = f.from_archetype_id
  and swc.to_archetype_id = f.to_archetype_id
  and swc.date = f.date
