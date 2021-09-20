
with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_policyline as(
    select 
    p.writtenline,
    p.estsigningdown,
    p.actualsigningdown,
    p.wholepartorder,
    p.writtenorder,
    p.signednum,
    p.signedorder,
    p.policylineref,
    p.linestatus,
    p.effectivesignedline,
    p.signeddate,
    p.renewalcode,
    p.policylineid,
    p.policyid,
    p.synd,
    p.producingteam
    ,case when actual_date >= valid_from and actual_date <= valid_to and deldate is null then 1 else 0 end as in_date
    ,actual_date
    from 
    policyline p
    cross join cte_date_all d

)
select 
    p.writtenline,
    p.estsigningdown,
    p.actualsigningdown,
    p.wholepartorder,
    p.writtenorder,
    p.signednum,
    p.signedorder,
    p.policylineref,
    p.linestatus,
    p.effectivesignedline,
    p.signeddate,
    p.renewalcode,
    p.policylineid,
    p.policyid,
    p.actual_date,
    p.synd,
    p.producingteam

    from eff_range_policyline p
    where in_date = 1