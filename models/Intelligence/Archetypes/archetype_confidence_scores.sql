{{
  config(
    materialized='table',
    unique_key=['archetype_id', 'date']
  )
}}

with spine as (
  select
    a.archetype_id,
    d.date
  from {{ ref('archetype_definitions') }} a
  cross join {{ ref('dim_date') }} d
),

daily_agg as (
  select
    archetype_id,
    date,
    count(*) as cohort_count,
    countif(membership_confidence > 0) as contributing_cohort_count,
    avg(membership_confidence) as avg_confidence,
    avg(case when membership_confidence > 0 then membership_confidence end) as avg_contributing_confidence
  from {{ ref('cohort_to_archetype_mapping') }}
  group by archetype_id, date
),

with_ratios as (
  select
    s.archetype_id,
    s.date,
    coalesce(da.avg_confidence, 0) as raw_avg_confidence,
    coalesce(da.avg_contributing_confidence, 0) as evidence_density_raw,
    case
      when coalesce(da.cohort_count, 0) = 0 then 0
      else coalesce(da.contributing_cohort_count, 0) * 1.0 / da.cohort_count
    end as cohort_coverage_ratio
  from spine s
  left join daily_agg da on s.archetype_id = da.archetype_id and s.date = da.date
),

with_scores as (
  select
    archetype_id,
    date,
    least(1.0, greatest(0.0,
      raw_avg_confidence * cohort_coverage_ratio
    )) as confidence_score,
    least(1.0, greatest(0.0, evidence_density_raw)) as evidence_density,
    cohort_coverage_ratio
  from with_ratios
),

with_trend as (
  select
    archetype_id,
    date,
    confidence_score,
    evidence_density,
    cohort_coverage_ratio,
    avg(confidence_score) over (
      partition by archetype_id
      order by date
      rows between 7 preceding and 1 preceding
    ) as prior_7d_avg
  from with_scores
),

with_trend_label as (
  select
    archetype_id,
    date,
    confidence_score,
    evidence_density,
    cohort_coverage_ratio,
    case
      when prior_7d_avg is null then 'stable'
      when confidence_score > prior_7d_avg then 'rising'
      when confidence_score < prior_7d_avg then 'declining'
      else 'stable'
    end as confidence_trend
  from with_trend
)

select
  archetype_id,
  date,
  confidence_score,
  confidence_trend,
  evidence_density,
  cohort_coverage_ratio,
  case
    when confidence_score >= 0.5
      and evidence_density >= 0.5
      and cohort_coverage_ratio >= 0.5
      and confidence_trend = 'stable'
    then 'allowed'
    when confidence_trend = 'rising'
      and not (
        confidence_score >= 0.5
        and evidence_density >= 0.5
        and cohort_coverage_ratio >= 0.5
      )
    then 'exploratory'
    else 'blocked'
  end as action_readiness_level
from with_trend_label
