
-- Policy Staging

--SOURCE: CURATED SNAPSHOT TABLE

with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_policy as(
    select 
      p.policyid,
    p.layernum,
    p.instalmentperiod,
    p.mainlayerind,
    p.policystatus,
    p.policyref,
    p.sectioncode,
    p.inceptiondate,
    p.expirydate,
    p.canceldate,
    p.policytype,
    p.bureausettledind,
    p.renewedfromref,
    p.periodtype,
    p.datewritten,
    p.uniquemarketref,
    p.leadind,
    p.yoa,
    p.class1,
    p.class2,
    p.class3,
    p.class4
    ,case when actual_date >= valid_from and actual_date <= valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    policy p
    cross join cte_date_all d

)
select 
    p.actual_date,
    p.policyid,
    p.layernum,
    p.instalmentperiod,
    p.mainlayerind,
    p.policystatus,
    p.policyref,
    p.sectioncode,
    p.inceptiondate,
    p.expirydate,
    p.canceldate,
    p.policytype,
    p.bureausettledind,
    p.renewedfromref,
    p.periodtype,
    p.datewritten,
    p.uniquemarketref,
    p.leadind,
    p.yoa,
    p.class1,
    p.class2,
    p.class3,
    p.class4

    from eff_range_policy p
    where in_date = 1