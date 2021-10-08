-- Create a "as at day" view of the ECLIPSE Claims Event table


--SOURCE: CURATED SNAPSHOT TABLE

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),

earliest_date as (

select claimeventid, min(valid_from) as valid_from from claimevent
group by claimeventid

),
is_first_row as (

select claimeventid,valid_from,dense_rank() over (partition by claimeventid order by valid_from) rankit
from claimevent

),
eff_range_source as(
    select 
    p.*,
    case  when actual_date <= ed.valid_from and f.rankit = 1 then 1 -- small fix for claims that have movements before snapshot say they existed. Joy.
          when actual_date >= p.valid_from and actual_date <= p.valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    claimevent p
    cross join cte_date_all d
        join earliest_date ed on
            p.claimeventid = ed.claimeventid
        left join is_first_row f on
            p.claimeventid = f.claimeventid
            and p.valid_from = f.valid_from 
)
select *

    from eff_range_source p
    where in_date = 1