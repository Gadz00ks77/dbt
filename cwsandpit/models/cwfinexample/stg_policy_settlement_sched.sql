
with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_source as(
    select 
    p.policysettschedid,
    p.policyid,
    p.instalmentnum,
    p.instalmenttype,
    p.roe,
    p.lastupd,
    p.deldate,
    p.origccyiso as default_origccyiso --deductions don't have orig currencies for some reason, so we'll take them from here later. 
    ,case when actual_date >= valid_from and actual_date <= valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    policysettsched p
    cross join cte_date_all d

)
select 
    p.policysettschedid,
    p.policyid,
    p.actual_date,
    p.instalmentnum,
    p.instalmenttype,
    p.roe,
    p.lastupd,
    p.deldate,
    default_origccyiso
    from eff_range_source p
    where in_date = 1