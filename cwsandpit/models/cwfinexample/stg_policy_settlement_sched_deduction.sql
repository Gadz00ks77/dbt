

--SOURCE: CURATED SNAPSHOT TABLE

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_source as(
    select 
    p.*
    ,case when actual_date >= valid_from and actual_date <= valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    settlementschedulededuction p
    cross join cte_date_all d

)
select 
    p.settlementscheduledeductionid,
    p.policysettschedid,
    p.addition,
    p.fintranscategoryid,
    p.amtccyiso,
    p.actual_date,
    p.lastupd,
    p.deductionind,
    p.amtpct,
    p.deldate

    from eff_range_source p
    where in_date = 1