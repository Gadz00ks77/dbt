-- Create a "as at day" view of the ECLIPSE Claim Policy table


--SOURCE: CURATED SNAPSHOT TABLE

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),

earliest_date as (

select claimpolicyid, min(valid_from) as valid_from from claimpolicy
group by claimpolicyid

),
is_first_row as (

select claimpolicyid,valid_from,dense_rank() over (partition by claimpolicyid order by valid_from) rankit
from claimpolicy

),
eff_range_source as(
    select 
    p.*,
    case when actual_date <= ed.valid_from and f.rankit = 1  then 1 -- small fix for claims that have movements before snapshot say they existed. Joy.
     when actual_date >= p.valid_from and actual_date <= p.valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    claimpolicy p
    cross join cte_date_all d

        join earliest_date ed on
            p.claimpolicyid = ed.claimpolicyid
        left join is_first_row f on
            p.claimpolicyid = f.claimpolicyid
            and p.valid_from = f.valid_from 
)
select *

    from eff_range_source p
    where in_date = 1