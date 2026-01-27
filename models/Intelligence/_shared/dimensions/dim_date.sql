{{
  config(
    materialized='table',
    unique_key='date'
  )
}}

/*
  Model: dim_date
  Layer: Shared dimension (foundational infrastructure)
  Grain: One row per calendar day
  Purpose: Canonical calendar-day ontology for temporal operations
*/

with date_spine_params as (
  select
    date('2020-01-01') as min_date,
    date_add(current_date(), interval 365 day) as max_date
),

date_spine as (
  select
    date_add(
      (select min_date from date_spine_params),
      interval day_offset day
    ) as date
  from 
    unnest(
      generate_array(
        0,
        date_diff(
          (select max_date from date_spine_params),
          (select min_date from date_spine_params),
          day
        )
      )
    ) as day_offset
),

date_attributes as (
  select
    ds.date,
    
    -- Calendar structure
    extract(day from ds.date) as day_of_month,
    extract(dayofweek from ds.date) as day_of_week,
    extract(week from ds.date) as week_of_year,
    extract(month from ds.date) as month,
    format_date('%B', ds.date) as month_name,
    extract(quarter from ds.date) as quarter,
    extract(year from ds.date) as year,
    
    -- Ordering & comparability
    format_date('%Y%m%d', ds.date) as date_key,
    date_diff(ds.date, (select min_date from date_spine_params), day) as day_index,
    
    -- Week boundaries
    date_trunc(ds.date, week(sunday)) as week_start_date,
    date_add(date_trunc(ds.date, week(sunday)), interval 6 day) as week_end_date,
    
    -- Month boundaries
    date_trunc(ds.date, month) as month_start_date,
    last_day(ds.date, month) as month_end_date,
    
    -- Quarter boundaries
    date_trunc(ds.date, quarter) as quarter_start_date,
    last_day(date_trunc(ds.date, quarter), quarter) as quarter_end_date,
    
    -- Year boundaries
    date_trunc(ds.date, year) as year_start_date,
    date(extract(year from ds.date), 12, 31) as year_end_date,
    
    -- Weekday/weekend flags
    case
      when extract(dayofweek from ds.date) in (1, 7) then true
      else false
    end as is_weekend,
    
    case
      when extract(dayofweek from ds.date) not in (1, 7) then true
      else false
    end as is_weekday
    
  from date_spine ds
)

-- Final output: one row per calendar day
select
  date,
  
  -- Calendar structure
  day_of_month,
  day_of_week,
  week_of_year,
  month,
  month_name,
  quarter,
  year,
  
  -- Ordering & comparability
  date_key,
  day_index,
  
  -- Week boundaries
  week_start_date,
  week_end_date,
  
  -- Month boundaries
  month_start_date,
  month_end_date,
  
  -- Quarter boundaries
  quarter_start_date,
  quarter_end_date,
  
  -- Year boundaries
  year_start_date,
  year_end_date,
  
  -- Weekday/weekend flags
  is_weekend,
  is_weekday

from date_attributes
order by date