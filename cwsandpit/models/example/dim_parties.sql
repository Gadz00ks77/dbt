with cte_changed_rows as (
select 

o.orgid,
o.actual_date,
'eclipse' as source_system,
orgname,
orgstatus,
ifnull(o.orgname::text,'0')::text ||
ifnull(o.orgstatus::text,'0')::text 
 as change_key,
conditional_change_event(
ifnull(o.orgname::text,'0')::text ||
ifnull(o.orgstatus::text,'0')::text 
) over (partition by o.orgid order by o.actual_date) as change_num

from {{ref('stg_organisations')}} o 

),
cte_lag_it as (
select 
    c.*
    ,row_number() over (partition by orgid, change_num order by actual_date) rankit

    from cte_changed_rows c

),
cte_valids as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by orgid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it l
    where rankit = 1
),


cte_changed_rows2 as (
select 

lb.lloydsbrokerid,
lb.actual_date,
'eclipse' as source_system,
brokername,
brokercode::text as brokercode,
brokerpseud::text as brokerpseud,
ifnull(lb.brokername::text,'0')::text ||
ifnull(lb.brokercode::text,'0')::text ||
ifnull(lb.brokerpseud::text,'0')
 as change_key,
conditional_change_event(
ifnull(lb.brokername::text,'0')::text ||
ifnull(lb.brokercode::text,'0')::text ||
ifnull(lb.brokerpseud::text,'0')::text 
) over (partition by lb.lloydsbrokerid order by lb.actual_date) as change_num

from {{ref('stg_lloyds_brokers')}} lb 

),
cte_lag_it2 as (
select 
    c.*
    ,row_number() over (partition by lloydsbrokerid, change_num order by actual_date) rankit

    from cte_changed_rows2 c

),
cte_valids2 as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by lloydsbrokerid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it2 l
    where rankit = 1
)


select 
sha2(v.orgid::text||
v.valid_to::text) as party_key,
v.orgid::text as party_nk,
v.valid_from as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.orgname as party_name,
v.orgstatus as party_status,
'n/a' as party_pseud,
'n/a' as party_code
from 
cte_valids v

union 
select distinct 
sha2(
ps.synd::text
) as party_key,
ps.synd::text as party_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'eclipse' as source_system,
synd as party_name,
'n/a' as party_status,
'n/a' as party_pseud,
'n/a' as party_code

from
{{ref('stg_policylines')}} ps
where ps.synd is not null 

union
select distinct 
sha2(
ps.producingteam::text
) as party_key,
ps.producingteam::text as party_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'eclipse' as source_system,
producingteam as party_name,
'n/a' as party_status,
'n/a' as party_pseud,
'n/a' as party_code

from
{{ref('stg_policylines')}} ps
where ps.producingteam is not null


union

select 
sha2(v.lloydsbrokerid::text||'broker'||
v.valid_to::text) as party_key,
v.lloydsbrokerid::text||'broker' as party_nk,
v.valid_from as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.brokername as party_name,
'n/a' as party_status,
v.brokerpseud::text as party_pseud,
v.brokercode::text as party_code
from 
cte_valids2 v

union

select 
sha2('n/a') as party_key,
'n/a' as party_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Not Applicable' as party_name,
'n/a' as party_status,
'n/a' as party_pseud,
'n/a' as party_code

union

select 
sha2('Unknown') as party_key,
'Unknown' as party_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Unknown' as party_name,
'n/a' as party_status,
'n/a' as party_pseud,
'n/a' as party_code
