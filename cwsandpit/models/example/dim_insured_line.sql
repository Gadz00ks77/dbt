{{ config(materialized='table') }}
with cte_changed_rows as
(
select 
    pl.policylineid,
    'eclipse' as source_system,
    pl.writtenline,
    pl.estsigningdown,
    pl.actualsigningdown,
    pl.wholepartorder,
    pl.writtenorder,
    pl.signednum,
    p.uniquemarketref,
    pl.policylineref,
    pl.linestatus,
    pl.effectivesignedline,
    pl.signedorder,
    p.datewritten,
    pl.signeddate,
    p.leadind,
    pl.renewalcode,
    pl.actual_date,
    ifnull(pl.writtenline::text,'0') ||
    ifnull(pl.estsigningdown::text,'0') ||
    ifnull(pl.actualsigningdown::text,'0') ||
    ifnull(pl.wholepartorder::text,'0') ||
    ifnull(pl.writtenorder::text,'0') ||
    ifnull(pl.signednum::text,'0') ||
    ifnull(pl.signedorder::text,'0') ||
    ifnull(p.uniquemarketref::text,'0') ||
    ifnull(pl.policylineref::text,'0') ||
    ifnull(pl.linestatus::text,'0') ||
    ifnull(pl.effectivesignedline::text,'0') ||
    ifnull(p.datewritten::text,'0') ||
    ifnull(pl.signeddate::text,'0') ||
    ifnull(p.leadind::text,'0') ||
    ifnull(pl.renewalcode::text,'0') as change_key,
    conditional_change_event(
    ifnull(pl.writtenline::text,'0') ||
    ifnull(pl.estsigningdown::text,'0') ||
    ifnull(pl.actualsigningdown::text,'0') ||
    ifnull(pl.wholepartorder::text,'0') ||
    ifnull(pl.writtenorder::text,'0') ||
    ifnull(pl.signednum::text,'0') ||
    ifnull(pl.signedorder::text,'0') ||
    ifnull(p.uniquemarketref::text,'0') ||
    ifnull(pl.policylineref::text,'0') ||
    ifnull(pl.linestatus::text,'0') ||
    ifnull(pl.effectivesignedline::text,'0') ||
    ifnull(p.datewritten::text,'0') ||
    ifnull(pl.signeddate::text,'0') ||
    ifnull(p.leadind::text,'0') ||
    ifnull(pl.renewalcode::text,'0')
    ) over (partition by pl.policylineid order by pl.actual_date) as change_num

from {{ ref('stg_policylines') }} pl 
    join {{ ref('stg_policies')}} p on 
        pl.policyid = p.policyid
        and pl.actual_date = p.actual_date

),
cte_lag_it as (
select 
    c.*
    ,row_number() over (partition by policylineid, change_num order by actual_date) rankit

    from cte_changed_rows c
),
cte_valids as (
select 
    l.*
    ,actual_date as valid_from
    ,ifnull(lead(actual_date) over (partition by policylineid order by change_num),'9999-12-31'::date) as valid_to

    from cte_lag_it l
    where l.rankit = 1
)
select 
sha2(v.policylineid||
v.valid_to::text) as insured_line_key,
v.policylineid::text as insured_line_nk,
v.valid_from as effective_from,
case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
v.change_key,
v.source_system,
v.policylineref as Policy_Reference,
v.writtenline,
v.estsigningdown as Estimated_Signing,
v.actualsigningdown as Actual_Signing,
v.wholepartorder as Whole_Or_Order,
v.writtenorder as Written_Order,
v.signednum as Signing_Num,
v.uniquemarketref as Unique_Market_Reference,
v.linestatus as Line_Status,
v.signedorder as Signed_Order,
v.effectivesignedline as Effective_Line,
v.datewritten as Date_Written,
v.signeddate as Date_Signed,
v.leadind as Is_Market_Lead,
v.renewalcode as Renewal_Intention


from 
cte_valids v

union 

select 
sha2('n/a') as insured_line_key,
'n/a' as insured_line_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Not Available' as Policy_Reference,
null as writtenline,
null as Estimated_Signing,
null as Actual_Signing,
null as Whole_Or_Order,
null as Written_Order,
null as Signing_Num,
null as Unique_Market_Reference,
null as Line_Status,
null as Signed_Order,
null as Effective_Line,
null as Date_Written,
null as Date_Signed,
null as Is_Market_Lead,
null as Renewal_Intention

union 

select 
sha2('Unknown') as insured_line_key,
'Unknown'as insured_line_nk,
'1901-01-01'::date as effective_from,
'9999-12-31'::date as effective_to,
1 is_current_row,
'n/a' as change_key,
'n/a' as source_system,
'Unknown' as Policy_Reference,
null as writtenline,
null as Estimated_Signing,
null as Actual_Signing,
null as Whole_Or_Order,
null as Written_Order,
null as Signing_Num,
null as Unique_Market_Reference,
null as Line_Status,
null as Signed_Order,
null as Effective_Line,
null as Date_Written,
null as Date_Signed,
null as Is_Market_Lead,
null as Renewal_Intention
