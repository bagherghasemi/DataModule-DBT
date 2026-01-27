-- Test: Check for gaps in date sequence
-- Should return 0 rows if dates are continuous

with date_with_previous as (
  select
    date_key,
    parse_date('%Y%m%d', date_key) as date_value,
    lag(parse_date('%Y%m%d', date_key)) over (order by date_key) as previous_date_value
  from {{ ref('dim_date') }}
)

select
  date_key
from date_with_previous
where date_value != date_add(previous_date_value, interval 1 day)
  and previous_date_value is not null