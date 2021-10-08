-- Create a "as at day" view of the ECLIPSE claims table


--SOURCE: CURATED SNAPSHOT TABLE

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),

earliest_date as (

select claimid, min(valid_from) as valid_from from eclipseclaim
group by claimid

),
is_first_row as (

select claimid,valid_from,dense_rank() over (partition by claimid order by valid_from) rankit
from eclipseclaim

),

eff_range_source as(
    select 
    p.*
    ,case 
      when actual_date <= ed.valid_from and f.rankit = 1 then 1 -- small fix for claims that have movements before snapshot say they existed. Joy.
      when actual_date >= p.valid_from and actual_date <= p.valid_to and deldate is null then 1 else 0 end as in_date -- if a row is deleted it's no longer effective.
    ,actual_date
    from 
    eclipseclaim p
    cross join cte_date_all d

        join earliest_date ed on
            p.claimid = ed.claimid
        left join is_first_row f on
            p.claimid = f.claimid
            and p.valid_from = f.valid_from 

)
select *

    from eff_range_source p
    where in_date = 1