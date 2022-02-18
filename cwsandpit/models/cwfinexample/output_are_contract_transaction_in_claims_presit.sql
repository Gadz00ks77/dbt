

with cte_informat as (
select 
substring(DATE_TRUNC(quarter,snapshot_date::date)::text,1,7) as yrq, -- not normally needed
min(snapshot_date) over (partition by il.policy_reference,substring(DATE_TRUNC(quarter,snapshot_date::date)::text,1,7)) as first_q_date, -- not normally needed
substring(DATE_TRUNC(month,snapshot_date::date)::text,1,7) as monthq, -- not normally needed
min(snapshot_date) over (partition by il.policy_reference,substring(DATE_TRUNC(month,snapshot_date::date)::text,1,7)) as first_m_date, -- not normally needed
null                    as ID,
null                    as LPG_ID,
null                    as EVENT_STATUS,
null                    as EVENT_ERROR_STRING, 
NULL                    as NO_RETRIES,
NULL                    as STAN_RULE_IDENT,
NULL                    as PROCESS_ID,
NULL                    as SUB_SYSTEM_ID,
NULL                    as MESSAGE_ID,
'FDU'                   as REMITTING_SYSTEM_ID,
NULL                    as ARRIVAL_TIME,
NULL                    as SOURCE_TRAN_NO,
ws.periodic_key         as SOURCE_TRAN_VER,
NULL                    as EVENT_AUDIT_ID,
NULL                    as BUSINESS_DATE,
ws.source_system        as SOURCE_SYS_INST_CODE, -- Ultimate Transaction Source (Eclipse)
NULL                    as STATIC_SYS_INST_CODE,
NULL                    as PARTY_SYS_INST_CODE,
NULL                    as CONTRACT_SYS_INST_CODE,
NULL                    as ACTIVE,
NULL                    as INPUT_BY,
NULL                    as INPUT_TIME,
NULL                    as AE_ACC_EVENT_TYPE_ID,
NULL                    as AE_SUB_EVENT_ID,
ws.snapshot_date        as AE_ACCOUNTING_DATE, -- Movement Date
NULL                    as AE_POS_NEG,
NULL                    as AE_DIMENSION_1, 
ws.uw_year_key          as AE_DIMENSION_2, -- Uw Year
NULL                    as AE_DIMENSION_3, -- Acc Year
cob.tier_1_code         as AE_DIMENSION_4, -- COB Tier 1
'TBC'                   as AE_DIMENSION_5, -- Target Market
'000'                   as AE_DIMENSION_6, -- Intercompany
r.placing_basis         as AE_DIMENSION_7, -- Placing Basis
r.contract_type         as AE_DIMENSION_8, -- Contract Type
NULL                    as AE_DIMENSION_9, 
NULL                    as AE_DIMENSION_10,
cob.tier_3_code         as AE_DIMENSION_11, -- COB Tier 3
NULL                    as AE_DIMENSION_12,
NULL                    as AE_DIMENSION_13, -- RI Contract
NULL                        as AE_DIMENSION_14, -- Reinsurer
NULL                        as AE_DIMENSION_15,
NULL                        as DESCRIPTION,
NULL                        as PARENT_TRAN_NO,
NULL                        as PARENT_TRAN_VER,
NULL                        as PARENT_TYPE,
il.policy_reference         as CONTRACT_CLICODE, -- Insured Line Eclipse Policy Reference
NULL                        as CONTRACT_PART_CLICODE,
NULL                        as TRANSACTION_TYPE_CLICODE,
NULL                        as BO_BOOK_CLICODE,
NULL                        as IPE_ENTITY_CLIENT_CODE,
case when le.party_name = 'CVIU' then 130 else 120 end as PL_PARTY_LEGAL_CLICODE, -- Legal Entity
NULL                        as PBU_PARTY_BUS_CLIENT_CODE,
sc.isochar_code             as CU_CURRENCY_ISO_CODE, -- Settlement Currency Code
NULL                        as GROSS_AMOUNT,
NULL                        as NET_AMOUNT,
NULL                        as TAX_AMOUNT,
NULL                        as TAX_CODE1,
NULL                        as TAX_CODE2,
NULL                        as TAX_CODE3,
NULL                        as INVOICE_DATE,
ap.accounting_period        as OTHER_DATE1, -- Accounting Date
case when ws.date_of_loss_key is not null then ws.date_of_loss_key 
    when te.transaction_event = 'Inward Claims' and ws.date_of_loss_key is null then dateadd(month,3,ws.inception_date_key)
    else null end
       as OTHER_DATE2,
NULL                        as TOTAL_AMOUNT,
te.transaction_event        as CLIENT_TEXT1, -- Transaction Event
te.transaction_sub_event    as CLIENT_TEXT2, -- Transaction Sub Event
fc.parent_category          as CLIENT_TEXT3, -- Financial Parent Category
fc.category                 as CLIENT_TEXT4, -- Financial Category
fc.sub_category             as CLIENT_TEXT5, -- Financial Sub Category
ws.is_addition              as CLIENT_TEXT6, -- Is Addition Marker
cl.claim_reference          as CLIENT_TEXT7, -- Peril Code (N/A for Premium) CHANGED TO CLAIM_REFERENCE 17/01/22
'n/a'                       as CLIENT_TEXT8, -- New Policy Marker (To Be Evaluated Later)
oc.isochar_code             as CLIENT_TEXT9, -- Original Currency Code
NULL                        as CLIENT_TEXT10,
NULL                        as CLIENT_TEXT11,
NULL                        as CLIENT_TEXT12,
NULL                        as CLIENT_TEXT13,
NULL                        as CLIENT_TEXT14,
ifnull(pd.peril_code,'n/a') as CLIENT_TEXT15,
NULL                        as CLIENT_TEXT16,
NULL                        as CLIENT_TEXT17,
NULL                        as CLIENT_TEXT18,
NULL                        as CLIENT_TEXT19,
il.line_status              as CLIENT_TEXT20, -- Insured Line Status
sum(ws.measure_sett)::decimal(15,2)        as CLIENT_AMOUNT1, -- Settlement Currency Snapshot Amount
sum(ws.measure_orig)::decimal(15,2)        as CLIENT_AMOUNT2, -- Original Currency Snapshot Amount
NULL                        as CLIENT_AMOUNT3,
NULL                        as CLIENT_AMOUNT4,
NULL                        as CLIENT_AMOUNT5,
NULL                        as CLIENT_AMOUNT6,
NULL                        as CLIENT_AMOUNT7,
NULL                        as CLIENT_AMOUNT8,
NULL                        as CLIENT_AMOUNT9,
NULL                        as CLIENT_AMOUNT10,
NULL                        as AE_VALUE_DATE,
NULL                        as LOCAL_AMOUNT,
NULL                        as LOCAL_CU_CURRENCY_ISO_CODE

from
{{ref('fct_trans_periodic_snapshot')}} ws

    join {{ref('dim_insured_line')}} il
        on ws.insured_line_key = il.insured_line_key and il.policy_reference = 'AA498K19B000'

    join {{ref('dim_perils')}} pd  
        on ws.claim_peril_key = pd.peril_key

    join dim_class_of_business cob 
        on ws.class_of_business_key = cob.class_of_business_key

    join {{ref('dim_claims')}} cl
        on ws.claim_key = cl.claim_key

    join {{ref('dim_risk')}} r    
        on r.risk_key = ws.risk_key

    join {{ref('dim_parties')}} le 
        on ws.legal_entity_key = le.party_key

    join {{ref('dim_currencies')}} sc   
        on ws.sett_ccy_key = sc.currency_key

    join {{ref('dim_currencies')}} oc   
        on ws.orig_ccy_key = oc.currency_key

    join {{ref('dim_transaction_events')}} te
        on te.transaction_event_key = ws.transaction_event_key
        and te.transaction_sub_event not like '%Market%'
        and te.transaction_event in ('Inward Claims')

    join {{ref('dim_financial_categories')}} fc 
        on fc.financial_category_key = ws.financial_category_key

    join dim_accounting_periods ap
        on ws.accounting_period_key = ap.accounting_period_key

    -- where 
    --       upper(cob.tier_1_name) = upper('Property')
    --       or upper(cob.tier_1_name) like 'ACC%'
    --       or cob.tier_2_name = 'A&H'



group by 

    ws.periodic_key,
    ws.source_system,
    ws.snapshot_date,
    ws.uw_year_key,
    r.placing_basis,
    r.contract_type,
    il.policy_reference,
    le.party_name,
    sc.isochar_code,
    cl.claim_reference,
    ap.accounting_period,
    te.transaction_event,
    te.transaction_sub_event,
    fc.parent_category,
    fc.category,
    fc.sub_category,
    ws.is_addition,
    pd.peril_code,
    oc.isochar_code,
    il.line_status,
    ws.date_of_loss_key,
    cob.tier_1_code,
    cob.tier_3_code,
    ws.inception_date_key

), cte_cadence as 

(

    select * 
    from cte_informat i

    -- where i.ae_accounting_date in
    -- (
    --   '2021-06-01',
    --   '2021-07-01',
    --   '2021-08-01'
    -- )

        
), cte_capture_change as (

select 
    ae_accounting_date              as snapshot_date,
  --client_text20,
    contract_clicode||'|'||
    client_text1||'|'||
    client_text2||'|'||
    client_text3||'|'||
    client_text4||'|'||
    client_text5||'|'||
    client_text6||'|'||
    client_text7||'|'||
    client_text8||'|'||
    client_text9||'|'||
    CU_CURRENCY_ISO_CODE
                                    as nk_transin
    ,other_date1                    as accounting_period
    ,conditional_change_event(
            client_amount1::text||'|'||
            client_amount2::text
        ) over
        (
        partition by 
          ifnull(contract_clicode,'0'),
          ifnull(client_text1,'0'),
          ifnull(client_text2,'0'),
          ifnull(client_text3,'0'),
          ifnull(client_text4,'0'),
          ifnull(client_text5,'0'),
          ifnull(client_text6,'0'),
          ifnull(client_text7,'0'),
          ifnull(client_text8,'0'),
          ifnull(client_text9,'0'),
          ifnull(CU_CURRENCY_ISO_CODE,'0')
        order by
          ae_accounting_date

        )                           as change_marker_amount
    ,conditional_change_event(
            ifnull(ae_dimension_2,'0')||'|'||
            ifnull(ae_dimension_4,'0')||'|'||
            ifnull(ae_dimension_5,'0')||'|'||
            ifnull(ae_dimension_6,'0')||'|'||
            ifnull(ae_dimension_7,'0')||'|'||
            ifnull(ae_dimension_8,'0')||'|'||
            ifnull(ae_dimension_11,'0')||'|'||
            ifnull(ae_dimension_13,'0')||'|'||
            ifnull(ae_dimension_14,'0')||'|'||
            ifnull(client_text15,'0')||'|'||
            ifnull(pl_party_legal_clicode,'0')||'|'||
            ifnull(client_text10,'0')||'|'||
            ifnull(client_text20,'0')||'|'||
            ifnull(OTHER_DATE2,'0')
        ) over
        (
        partition by 
          contract_clicode||'|'||client_text7
        order by
          ae_accounting_date

        )                           as change_marker_policy_only
  ,dense_rank(
        ) over
        (
        partition by 
          contract_clicode
        order by
          ae_accounting_date

        )      as change_pol_level
    ,i.*

from cte_cadence i

-- where 
--         ap.day_of_month = 1 -- this is removed as I want to produce one "specific date" snapshot
--           ap.date::date =  DATE_TRUNC(quarter,ap.date::date)
--           or ap.date::date = i.first_q_date
--where ae_accounting_date = '2021-11-16'  


order by 
        accounting_period
        ,snapshot_date

),

distinct_targmarkets as ( -- this should not be necessary, but there are dupes on the file received from Actuarial

select distinct lineref,targetmarket from {{ref('targetmarket_mapping')}} 

),

cte_lagged as (

select 
  ifnull(lag(change_marker_amount) over (partition by nk_transin order by snapshot_date),-1) as lagged_change_marker_amount
  ,ifnull(lag(change_marker_policy_only) over (partition by nk_transin order by snapshot_date),-1) as lagged_change_marker_policy_only
  , * 
from cte_capture_change
  )
  
select 

nk_transin as _nk_transin,
snapshot_date as _snapshot_date,
EVENT_STATUS,
EVENT_ERROR_STRING, 
NO_RETRIES,
STAN_RULE_IDENT,
PROCESS_ID,
SUB_SYSTEM_ID,
MESSAGE_ID,
REMITTING_SYSTEM_ID,
ARRIVAL_TIME,
SOURCE_TRAN_NO,
SOURCE_TRAN_VER,
EVENT_AUDIT_ID,
BUSINESS_DATE,
SOURCE_SYS_INST_CODE,
STATIC_SYS_INST_CODE,
PARTY_SYS_INST_CODE,
CONTRACT_SYS_INST_CODE,
ACTIVE,
INPUT_BY,
INPUT_TIME,
AE_ACC_EVENT_TYPE_ID,
AE_SUB_EVENT_ID,
AE_ACCOUNTING_DATE::datetime AE_ACCOUNTING_DATE, -- Movement Date
AE_POS_NEG,
AE_DIMENSION_1, 
substring(AE_DIMENSION_2,1,4) AE_DIMENSION_2, -- Uw Year
AE_DIMENSION_3, -- Acc Year
AE_DIMENSION_4, -- Mythic "COB" Nonsense
case when tmm.targetmarket is null and ae_dimension_4 = 'PROP' Then 'Prop Re US Cat XL'
when tmm.targetmarket is null and ae_dimension_4 = 'SPEC' Then 'Spec Re Other'
else tmm.targetmarket end
as AE_DIMENSION_5, -- Target Market
AE_DIMENSION_6, -- Intercompany
AE_DIMENSION_7, -- Placing Basis
AE_DIMENSION_8, -- Contract Type
AE_DIMENSION_9, 
AE_DIMENSION_10,
AE_DIMENSION_11, -- More "COB" Stuff
AE_DIMENSION_12,
AE_DIMENSION_13, -- RI Contract
AE_DIMENSION_14, -- Reinsurer
AE_DIMENSION_15,
DESCRIPTION,
PARENT_TRAN_NO,
PARENT_TRAN_VER,
PARENT_TYPE,
CONTRACT_CLICODE, -- Insured Line Eclipse Policy Reference
CONTRACT_PART_CLICODE,
TRANSACTION_TYPE_CLICODE,
BO_BOOK_CLICODE,
IPE_ENTITY_CLIENT_CODE,
PL_PARTY_LEGAL_CLICODE, -- Legal Entity
PBU_PARTY_BUS_CLIENT_CODE,
CU_CURRENCY_ISO_CODE, -- Settlement Currency Code
GROSS_AMOUNT,
NET_AMOUNT,
TAX_AMOUNT,
TAX_CODE1,
TAX_CODE2,
TAX_CODE3,
INVOICE_DATE,
(substring(OTHER_DATE1::text,1,4)||'-'||substring(OTHER_DATE1::text,5,6)||'-01')::datetime as OTHER_DATE1, -- Accounting Date
OTHER_DATE2,
TOTAL_AMOUNT,
CLIENT_TEXT1, -- Transaction Event
CLIENT_TEXT2, -- Transaction Sub Event
CLIENT_TEXT3, -- Financial Parent Category
CLIENT_TEXT4, -- Financial Category
CLIENT_TEXT5, -- Financial Sub Category
CLIENT_TEXT6, -- Is Addition Marker
CLIENT_TEXT7, -- Peril Code (N/A for Premium)
CLIENT_TEXT8, --case when change_pol_level = 1 then 'New' Else 'Not New' end as CLIENT_TEXT8, -- New Policy Marker -- deprecated
CLIENT_TEXT9, -- Original Currency Code
CLIENT_TEXT10,
CLIENT_TEXT11,
CLIENT_TEXT12,
CLIENT_TEXT13,
CLIENT_TEXT14,
CLIENT_TEXT15,
CLIENT_TEXT16,
CLIENT_TEXT17,
CLIENT_TEXT18,
CLIENT_TEXT19,
CLIENT_TEXT20, -- Insured Line Status
CLIENT_AMOUNT1, -- Settlement Currency Snapshot Amount
CLIENT_AMOUNT2, -- Original Currency Snapshot Amount
CLIENT_AMOUNT3,
CLIENT_AMOUNT4,
CLIENT_AMOUNT5,
CLIENT_AMOUNT6,
CLIENT_AMOUNT7,
CLIENT_AMOUNT8,
CLIENT_AMOUNT9,
CLIENT_AMOUNT10,
AE_VALUE_DATE,
LOCAL_AMOUNT,
LOCAL_CU_CURRENCY_ISO_CODE

from cte_lagged l

   --left join {{ref('are_sample_cobs')}} co on            
   --   l.contract_clicode = co.eclipse_policy_reference

    join distinct_targmarkets tmm on 
       tmm.lineref = l.contract_clicode

where (lagged_change_marker_amount != change_marker_amount or lagged_change_marker_policy_only != change_marker_policy_only)
order by _snapshot_date,_nk_transin