
with cte_changed_rows as (
select 

cl.actual_date,
cl.claimid,
'eclipse' as source_system,
cl.claimref,
cl.uniqueclaimref,
cl.brokerclaimref,
cl.losstitle,
ifnull(cl.uniqueclaimref::text,'0')::text ||
ifnull(cl.brokerclaimref::text,'0')::text ||
ifnull(cl.losstitle::text,'0')::text as change_key,
conditional_change_event(
ifnull(cl.uniqueclaimref::text,'0')::text ||
ifnull(cl.brokerclaimref::text,'0')::text ||
ifnull(cl.losstitle::text,'0')::text 
) over (partition by cl.claimid order by cl.actual_date) as change_num

from {{ ref('stg_claim') }}  cl  

),
cte_lag_it as (
select 
    c.*
    ,row_number() over (partition by claimid, change_num order by actual_date) rankit

    from cte_changed_rows c

),
cte_valids as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by claimid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it l
    where rankit = 1
)

select 
sha2(v.claimid||
v.valid_to::text) as claim_key,
v.claimid::text as claim_nk,
v.valid_from as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.claimref as Claim_Reference,
v.uniqueclaimref as Unique_Claim_Reference,
v.brokerclaimref as Broker_Claim_Reference,
v.losstitle as Claim_Title
from 
cte_valids v

--adding some "Not Available" and "Unknown" values

union

select 
sha2('n/a') as claim_key,
'n/a' as claim_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Not Applicable' as Claim_Reference,
'n/a' as Unique_Claim_Reference,
'n/a' as Broker_Claim_Reference,
'n/a' as Claim_Title

union

select 
sha2('Unknown') as claim_key,
'Unknown' as claim_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Unknown' as Claim_Reference,
'n/a' as Unique_Claim_Reference,
'n/a' as Broker_Claim_Reference,
'n/a' as Claim_Title