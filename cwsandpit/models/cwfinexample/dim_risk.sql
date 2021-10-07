
--Originally this had a mapping to policypremium according to the AirTable. 
--PolicyPrem contains one row per policy event "change".
--So, for a policy that is set up there will be initially one row, but thereafter per endorsement or adjustable change - you will get a new row.
--Hence it's not correct to include policyprem on this structure.
--For an overall assessment of policy premium basis (i.e. Adjustable / Flat) - you can use the  policypremDETAIL table. Which is grained at the same level
--as policy and will thereafter not fan. (Not done here)

with cte_changed_rows as (
select 

p.policyid,
'eclipse' as source_system,
p.layernum,
--pp.basis, you can't put premium basis on the risk dimension as it kills the grain (shouldn't be here really)
p.instalmentperiod,
p.mainlayerind,
p.policystatus,
p.policyref,
p.sectioncode,
p.canceldate,
p.policytype,
p.bureausettledind,
p.renewedfromref,
p.periodtype,
p.actual_date,
p.placingtype,
ifnull(p.layernum::text,'0')::text ||
--ifnull(pp.basis::text,'0')::text ||
ifnull(p.instalmentperiod::text,'0')::text ||
ifnull(p.mainlayerind::text,'0')::text ||
ifnull(p.policystatus::text,'0')::text ||
ifnull(p.policyref::text,'0')::text ||
ifnull(p.sectioncode::text,'0')::text ||
ifnull(p.canceldate::text,'0')::text ||
ifnull(p.placingtype::text,'0')::text ||
ifnull(p.bureausettledind::text,'0')::text ||
ifnull(p.renewedfromref::text,'0')::text ||
ifnull(p.periodtype::text,'0')::text as change_key,
conditional_change_event(
ifnull(p.layernum::text,'0')::text ||
--ifnull(pp.basis::text,'0')::text ||
ifnull(p.instalmentperiod::text,'0')::text ||
ifnull(p.mainlayerind::text,'0')::text ||
ifnull(p.policystatus::text,'0')::text ||
ifnull(p.policyref::text,'0')::text ||
ifnull(p.sectioncode::text,'0')::text ||
ifnull(p.canceldate::text,'0')::text ||
ifnull(p.placingtype::text,'0')::text ||
ifnull(p.bureausettledind::text,'0')::text ||
ifnull(p.renewedfromref::text,'0')::text ||
ifnull(p.periodtype::text,'0')::text
) over (partition by p.policyid order by p.actual_date) as change_num

from {{ ref('stg_policies') }}  p 
  --  join architecture_db.cwsandpit.stg_policyprem pp on -- a link to policy prem is incorrect (it fans)
  --      p.policyid = pp.policyid
  --      and p.actual_date = pp.actual_date
  

),
cte_lag_it as (
select 
    c.*
    ,row_number() over (partition by policyid, change_num order by actual_date) rankit

    from cte_changed_rows c

),
cte_valids as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by policyid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it l
    where rankit = 1
)

select 
sha2(v.policyid||
v.valid_to::text) as risk_key,
v.policyid as risk_nk,
case when v.valid_from = '2019-12-31' then '1901-01-01' else v.valid_from end as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.layernum  as layer_number,
v.instalmentperiod as instalment_period,
v.mainlayerind as is_main_layer,
v.policystatus as layer_status,
v.policyref as convex_risk_reference,
v.sectioncode as risk_section_identifier,
v.canceldate as cancellation_date,
v.placingtype as placing_basis,
pl.contracttypecode as contract_type,
v.bureausettledind as is_bureau,
v.policystatus as risk_status,
v.renewedfromref as new_or_renewal,
v.periodtype as period_basis
from 
cte_valids v
  join {{ref('placingbasismapping')}} pl 
    on v.placingtype = pl.placingbasiscode