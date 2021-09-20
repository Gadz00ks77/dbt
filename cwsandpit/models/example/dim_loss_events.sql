
with cte_changed_rows as (
select 

cl.actual_date,
cl.claimeventid,
'eclipse' as source_system,
cl.eventname,
cl.dsc,
cl.eventcode,
cl.catcode,
ifnull(cl.eventname::text,'0')::text ||
ifnull(cl.dsc::text,'0')::text ||
ifnull(cl.catcode::text,'0')::text as change_key,
conditional_change_event(
ifnull(cl.eventname::text,'0')::text ||
ifnull(cl.dsc::text,'0')::text ||
ifnull(cl.catcode::text,'0')::text 
) over (partition by cl.claimeventid order by cl.actual_date) as change_num

from {{ ref('stg_claimevent') }}  cl

),
cte_lag_it as (
select 
    c.*
    ,row_number() over (partition by claimeventid, change_num order by actual_date) rankit

    from cte_changed_rows c

),
cte_valids as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by claimeventid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it l
    where rankit = 1
)

select 
sha2(v.claimeventid||
v.valid_to::text) as loss_event_key,
v.claimeventid::text as loss_event_nk,
v.valid_from as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.Eventname as Loss_Event_Name,
v.EventCode as Loss_Event_Code,
v.dsc as Loss_Event_Description,
v.catcode as PCS_Cat_Code
from 
cte_valids v

union

select 
sha2('n/a') as loss_event_key,
'n/a' as loss_event_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Not Applicable' as Loss_Event_Name,
'n/a' as Loss_Event_Code,
'n/a' as Loss_Event_Description,
'n/a' as PCS_Cat_Code

union

select 
sha2('Unknown') as loss_event_key,
'Unknown' as loss_event_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 as is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Unknown' as Loss_Event_Name,
'n/a' as Loss_Event_Code,
'n/a' as Loss_Event_Description,
'n/a' as PCS_Cat_Code
