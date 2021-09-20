{{ config(materialized='table') }}

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_policybroker as(
    select p.*
    ,case when actual_date >= valid_from and actual_date <= valid_to and deldate is null then 1 else 0 end as in_date
    ,actual_date
    from 
    policybroker p
    cross join cte_date_all d

)
select 
    p.*
    from eff_range_policybroker p
    where in_date = 1