-- stage the lloyds brokers table for each "day"
-- NOTE THE SENSITIVITY TO THE DELDATE CONDITION. This is a quick / naive solution, but resolved some fanning.


with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_source as(
    select p.*
    ,case when actual_date >= valid_from and actual_date <= valid_to and deldate is null then 1 else 0 end as in_date
    ,actual_date
    from 
    lloydsbroker p
    cross join cte_date_all d

)
select 
    p.*
    from eff_range_source p
    where in_date = 1