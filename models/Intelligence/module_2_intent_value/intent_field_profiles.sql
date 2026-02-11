{{
  config(
    materialized='table',
    unique_key='cohort_id'
  )
}}

with eligible_cohorts as (
  select distinct
    cohort_id
  from {{ ref('dim_cohort') }}
),

meta_signals as (
  select
    apd.ad_id,
    apd.ad_set_id,
    apd.campaign_id,
    apd.date,
    apd.impressions,
    apd.reach,
    apd.clicks,
    apd.unique_clicks,
    apd.inline_link_clicks,
    apd.outbound_clicks,
    apd.landing_page_views,
    apd.video_views,
    apd.thruplays,
    apd.post_engagements,
    apd.spend
  from {{ ref('analytics_ad_performance_daily') }} apd
  where apd.impressions > 0
),

ad_metadata as (
  select
    ad_id,
    ad_set_id,
    campaign_id,
    creative_id,
    creative_type
  from {{ ref('analytics_ads') }}
),

ad_set_metadata as (
  select
    ad_set_id,
    campaign_id,
    optimization_goal
  from {{ ref('analytics_ad_sets') }}
),

campaign_metadata as (
  select
    campaign_id,
    objective
  from {{ ref('analytics_campaigns') }}
),

enriched_signals as (
  select
    ms.*,
    am.creative_id,
    am.creative_type,
    asm.optimization_goal as ad_set_optimization_goal,
    cm.objective as campaign_objective
  from meta_signals ms
  left join ad_metadata am
    on ms.ad_id = am.ad_id
  left join ad_set_metadata asm
    on ms.ad_set_id = asm.ad_set_id
  left join campaign_metadata cm
    on ms.campaign_id = cm.campaign_id
),

global_aggregated_signals as (
  select
    sum(impressions) as global_total_impressions,
    sum(reach) as global_total_reach,
    sum(clicks) as global_total_clicks,
    sum(unique_clicks) as global_total_unique_clicks,
    sum(inline_link_clicks) as global_total_inline_link_clicks,
    sum(outbound_clicks) as global_total_outbound_clicks,
    sum(landing_page_views) as global_total_landing_page_views,
    sum(video_views) as global_total_video_views,
    sum(thruplays) as global_total_thruplays,
    sum(post_engagements) as global_total_post_engagements,
    sum(spend) as global_total_spend,
    count(distinct ad_id) as global_distinct_ad_count,
    count(distinct date) as global_distinct_date_count,
    avg(case when impressions > 0 then clicks / nullif(impressions, 0) else null end) as global_avg_ctr,
    stddev(case when impressions > 0 then clicks / nullif(impressions, 0) else null end) as global_stddev_ctr,
    avg(case when impressions > 0 then landing_page_views / nullif(impressions, 0) else null end) as global_avg_lpv_rate,
    stddev(case when impressions > 0 then landing_page_views / nullif(impressions, 0) else null end) as global_stddev_lpv_rate,
    avg(case when impressions > 0 then video_views / nullif(impressions, 0) else null end) as global_avg_video_view_rate,
    stddev(case when impressions > 0 then video_views / nullif(impressions, 0) else null end) as global_stddev_video_view_rate
  from enriched_signals
),

identity_context as (
  select
    cohort_id,
    sum(case when population_stage = 'exposure' then population_size else 0 end) as exposure_population,
    sum(case when population_stage = 'engagement' then population_size else 0 end) as engagement_population,
    sum(case when population_stage = 'purchase' then population_size else 0 end) as purchase_population,
    sum(case when population_stage = 'loyalty' then population_size else 0 end) as loyalty_population,
    sum(population_size) as total_population
  from {{ ref('identity_population_profiles') }}
  group by cohort_id
),

total_population_sum as (
  select
    sum(total_population) as global_total_population
  from identity_context
),

cohort_with_context as (
  select
    ec.cohort_id,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_impressions * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_impressions,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_reach * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_reach,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_clicks * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_clicks,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_unique_clicks * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_unique_clicks,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_inline_link_clicks * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_inline_link_clicks,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_outbound_clicks * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_outbound_clicks,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_landing_page_views * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_landing_page_views,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_video_views * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_video_views,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_thruplays * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_thruplays,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_post_engagements * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_post_engagements,
    case
      when coalesce(tps.global_total_population, 0) > 0
        then gas.global_total_spend * coalesce(ic.total_population, 0) / tps.global_total_population
      else 0.0
    end as total_spend,
    gas.global_distinct_ad_count as distinct_ad_count,
    gas.global_distinct_date_count as distinct_date_count,
    gas.global_avg_ctr as avg_ctr,
    gas.global_stddev_ctr as stddev_ctr,
    gas.global_avg_lpv_rate as avg_lpv_rate,
    gas.global_stddev_lpv_rate as stddev_lpv_rate,
    gas.global_avg_video_view_rate as avg_video_view_rate,
    gas.global_stddev_video_view_rate as stddev_video_view_rate,
    coalesce(ic.exposure_population, 0) as exposure_population,
    coalesce(ic.engagement_population, 0) as engagement_population,
    coalesce(ic.purchase_population, 0) as purchase_population,
    coalesce(ic.loyalty_population, 0) as loyalty_population
  from eligible_cohorts ec
  cross join global_aggregated_signals gas
  cross join total_population_sum tps
  left join identity_context ic
    on ec.cohort_id = ic.cohort_id
),

intent_axes as (
  select
    cohort_id,
    case
      when total_impressions > 0
        then least(1.0, greatest(0.0, 
          (coalesce(avg_ctr, 0) * 100 + 
           coalesce(avg_video_view_rate, 0) * 50 + 
           coalesce(total_post_engagements, 0) / nullif(total_impressions, 0) * 10) / 160.0
        ))
      else 0.0
    end as desire_emotional_intensity,
    case
      when total_impressions > 0
        then least(1.0, greatest(0.0,
          (coalesce(avg_lpv_rate, 0) * 100 + 
           coalesce(total_outbound_clicks, 0) / nullif(total_impressions, 0) * 50 + 
           coalesce(total_unique_clicks, 0) / nullif(total_impressions, 0) * 30) / 180.0
        ))
      else 0.0
    end as desire_volitional_strength,
    case
      when total_impressions > 0
        then least(1.0, greatest(0.0,
          (coalesce(total_video_views, 0) / nullif(total_impressions, 0) * 100 + 
           coalesce(total_thruplays, 0) / nullif(total_impressions, 0) * 50 + 
           coalesce(distinct_date_count, 0) / 30.0 * 20) / 170.0
        ))
      else 0.0
    end as desire_time_investment
  from cohort_with_context
),

composite_measures as (
  select
    ia.cohort_id,
    ia.desire_emotional_intensity,
    ia.desire_volitional_strength,
    ia.desire_time_investment,
    sqrt(
      power(ia.desire_emotional_intensity, 2) +
      power(ia.desire_volitional_strength, 2) +
      power(ia.desire_time_investment, 2)
    ) / sqrt(3.0) as expressed_desire,
    case
      when ia.desire_emotional_intensity + ia.desire_volitional_strength + ia.desire_time_investment > 0
        then 1.0 - (
          abs(ia.desire_emotional_intensity - ia.desire_volitional_strength) +
          abs(ia.desire_volitional_strength - ia.desire_time_investment) +
          abs(ia.desire_time_investment - ia.desire_emotional_intensity)
        ) / 3.0
      else 0.0
    end as desire_coherence,
    coalesce(cwc.stddev_ctr, 0) + coalesce(cwc.stddev_lpv_rate, 0) + coalesce(cwc.stddev_video_view_rate, 0) as desire_volatility
  from intent_axes ia
  left join cohort_with_context cwc
    on ia.cohort_id = cwc.cohort_id
)

select
  cohort_id,
  desire_emotional_intensity,
  desire_volitional_strength,
  desire_time_investment,
  expressed_desire,
  desire_coherence,
  desire_volatility
from composite_measures
