
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
    settlementschedulesharededuction p
    cross join cte_date_all d

)
select 
    p.settlementschedulesharedeductionid,
    p.settlementscheduledeductionid,
    p.deldate,
    case when p.deldate is not null then 0 else p.totalamt end as totalamt, -- see my note on policy settlement sched full
    p.actual_date,
    p.lastupd

    from eff_range_source p
    where in_date = 1