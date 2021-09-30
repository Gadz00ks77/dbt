
-- Very likely this needs enriching with multiple address sources in the future
-- At the moment this just addr from Eclipse

with cte_changed_rows as (
select 

a.addrid,
'eclipse' as source_system,
a.addrtype,
a.addr,
a.state,
a.postcode,
a.actual_date,
ifnull(a.addrtype::text,'0')::text ||
ifnull(a.addr::text,'0')::text ||
ifnull(a.state::text,'0')::text ||
ifnull(a.postcode::text,'0')::text 
as change_key,
conditional_change_event(
ifnull(a.addrtype::text,'0')::text ||
ifnull(a.addr::text,'0')::text ||
ifnull(a.state::text,'0')::text ||
ifnull(a.postcode::text,'0')::text 

) over (partition by a.addrid order by a.actual_date) as change_num

from {{ ref('stg_addr') }}  a   

),
cte_lag_it as (
select 
    c.*
    ,row_number() over (partition by addrid, change_num order by actual_date) rankit

    from cte_changed_rows c

),
cte_valids as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by addrid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it l
    where rankit = 1
)

select 
sha2(v.addrid||
v.valid_to::text) as address_key,
v.addrid::text as address_nk,
case when v.valid_from = '2019-12-31' then '1901-01-01' else v.valid_from end as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.addrtype  as address_type,
'n/a' as Building_Number_or_Name,
'n/a' as Suite,
v.addr as Street,
'n/a' as City,
v.State,
v.PostCode as Postal_Code
from 
cte_valids v


--adding some "Not Available" and "Unknown" values

union 

select 
sha2('n/a') as address_key,
'n/a'::text as address_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'eclipse' as source_system,
'n/a'  as address_type,
'n/a' as Building_Number_or_Name,
'n/a' as Suite,
'n/a' as Street,
'n/a' as City,
'n/a' as State,
'n/a' as Postal_Code

union 

select 
sha2('Unknown') as address_key,
'Unknown'::text as address_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'Unknown' as change_key,
'eclipse' as source_system,
'Unknown'  as address_type,
'Unknown' as Building_Number_or_Name,
'Unknown' as Suite,
'Unknown' as Street,
'Unknown' as City,
'Unknown' as State,
'Unknown' as Postal_Code
