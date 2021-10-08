-- Create a "as at day" view of the ECLIPSE claim line table


--SOURCE: CURATED SNAPSHOT TABLE

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),

earliest_date as (

select claimlineid, min(valid_from) as valid_from 
from claimline
group by claimlineid

),
is_first_row as (

select claimlineid,valid_from,dense_rank() over (partition by claimlineid order by valid_from) rankit
from claimline

),
eff_range_source as(
    select 
    p.*,
    case when actual_date <= ed.valid_from and f.rankit = 1 then 1 -- small fix for claims that have movements before snapshot say they existed. Joy.
      when actual_date >= p.valid_from and actual_date <= p.valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    claimline p
    cross join cte_date_all d
        join earliest_date ed on
            p.claimlineid = ed.claimlineid
        left join is_first_row f on
            p.claimlineid = f.claimlineid
            and p.valid_from = f.valid_from 
)
select *

    from eff_range_source p
    where in_date = 1