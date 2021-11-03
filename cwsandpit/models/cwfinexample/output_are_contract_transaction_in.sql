

with cte_informat as (
select 

null                    as ID,
null                    as LPG_ID,
'U'                     as EVENT_STATUS,
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
ws.date_of_loss_key     as AE_DIMENSION_3, -- Acc Year
'TBC'                   as AE_DIMENSION_4, -- Mythic "COB" Nonsense
'TBC'                   as AE_DIMENSION_5, -- Target Market
'TBC'                   as AE_DIMENSION_6, -- Intercompany
r.placing_basis         as AE_DIMENSION_7, -- Placing Basis
r.contract_type         as AE_DIMENSION_8, -- Contract Type
NULL                    as AE_DIMENSION_9, 
NULL                    as AE_DIMENSION_10,
'TBC'                   as AE_DIMENSION_11, -- More "COB" Stuff
NULL                    as AE_DIMENSION_12,
'TBC'                   as AE_DIMENSION_13, -- RI Contract
'TBC'                   as AE_DIMENSION_14, -- Reinsurer
NULL                    as AE_DIMENSION_15,
NULL                    as DESCRIPTION,
NULL                    as PARENT_TRAN_NO,
NULL                    as PARENT_TRAN_VER,
NULL                    as PARENT_TYPE,
il.policy_reference     as CONTRACT_CLICODE, -- Insured Line Eclipse Policy Reference
NULL                    as CONTRACT_PART_CLICODE,
NULL                    as TRANSACTION_TYPE_CLICODE,
NULL                    as BO_BOOK_CLICODE,
NULL                    as IPE_ENTITY_CLIENT_CODE,
le.party_name           as PL_PARTY_LEGAL_CLICODE, -- Legal Entity
NULL                    as PBU_PARTY_BUS_CLIENT_CODE,
sc.isochar_code         as CU_CURRENCY_ISO_CODE, -- Settlement Currency Code
NULL                    as GROSS_AMOUNT,
NULL                    as NET_AMOUNT,
NULL                    as TAX_AMOUNT,
NULL                    as TAX_CODE1,
NULL                    as TAX_CODE2,
NULL                    as TAX_CODE3,
NULL                    as INVOICE_DATE,
ap.accounting_period    as OTHER_DATE1, -- Accounting Date
NULL                    as OTHER_DATE2,
NULL                        as TOTAL_AMOUNT,
te.transaction_event        as CLIENT_TEXT1, -- Transaction Event
te.transaction_sub_event    as CLIENT_TEXT2, -- Transaction Sub Event
fc.parent_category          as CLIENT_TEXT3, -- Financial Parent Category
fc.category                 as CLIENT_TEXT4, -- Financial Category
fc.sub_category             as CLIENT_TEXT5, -- Financial Sub Category
ws.is_addition              as CLIENT_TEXT6, -- Is Addition Marker
pd.peril_code               as CLIENT_TEXT7, -- Peril Code (N/A for Premium)
'TBC'                       as CLIENT_TEXT8, -- New Policy Marker (To Be Evaluated Later)
oc.isochar_code             as CLIENT_TEXT9, -- Original Currency Code
'TBC'                       as CLIENT_TEXT10,
NULL                        as CLIENT_TEXT11,
NULL                        as CLIENT_TEXT12,
NULL                        as CLIENT_TEXT13,
NULL                        as CLIENT_TEXT14,
NULL                        as CLIENT_TEXT15,
NULL                        as CLIENT_TEXT16,
NULL                        as CLIENT_TEXT17,
NULL                        as CLIENT_TEXT18,
NULL                        as CLIENT_TEXT19,
il.line_status              as CLIENT_TEXT20, -- Insured Line Status
sum(ws.measure_sett)        as CLIENT_AMOUNT1, -- Settlement Currency Snapshot Amount
sum(ws.measure_orig)        as CLIENT_AMOUNT2, -- Original Currency Snapshot Amount
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
        on ws.insured_line_key = il.insured_line_key

    join {{ref('dim_perils')}} pd  
        on ws.claim_peril_key = pd.peril_key

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

    join {{ref('dim_financial_categories')}} fc 
        on fc.financial_category_key = ws.financial_category_key

    join dim_accounting_periods ap
        on ws.accounting_period_key = ap.accounting_period_key

    --join {{ref('are_sample')}} are_sample
    --    on il.policy_reference = are_sample.sampleset  -- LIMITING EVERYTHING TO THE SAMPLE DEFINED BY SS. CAN BE REMOVED LATER FOR ALL POLICIES.

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
    ws.date_of_loss_key

), cte_capture_change as (
select 
    ae_accounting_date              as snapshot_date,
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
            ae_dimension_4||'|'||
            ae_dimension_5||'|'||
            ae_dimension_6||'|'||
            ae_dimension_7||'|'||
            ae_dimension_8||'|'||
            ae_dimension_11||'|'||
            ae_dimension_13||'|'||
            ae_dimension_14||'|'||
            pl_party_legal_clicode||'|'||
            client_text10||'|'||
            client_text20||'|'||
            client_amount1::text||'|'||
            client_amount2::text
        ) over
        (
        partition by 
          contract_clicode,
          client_text1,
          client_text2,
          client_text3,
          client_text4,
          client_text5,
          client_text6,
          client_text7,
          client_text8,
          client_text9,
          CU_CURRENCY_ISO_CODE
        order by
          ae_accounting_date

        )                           as change_marker
    ,dense_rank(
        ) over
        (
        partition by 
          contract_clicode
        order by
          ae_accounting_date

        )      as change_pol_level
    ,i.*

from cte_informat i

    join {{ref('dim_date')}} ap 
        on ae_accounting_date::text = ap.date::text

where 
        --contract_clicode = 'BB142S20A000' and
        ap.day_of_month = 1

order by 
        accounting_period
        ,snapshot_date

),

distinct_targmarkets as ( -- this should not be necessary, but there are dupes on the file received from Actuarial

select distinct lineref,targetmarket from {{ref('targetmarket_mapping')}} 

),

cte_lagged as (

select ifnull(lag(change_marker) over (partition by nk_transin order by snapshot_date),-1) as lagged_change_marker, * 
from cte_capture_change
  )
  
select 

nk_transin as _nk_transin,
snapshot_date as _snapshot_date,
EVENT_ERROR_STRING, 
NO_RETRIES,
STAN_RULE_IDENT,
PROCESS_ID,
SUB_SYSTEM_ID,
MESSAGE_ID,
REMITTING_SYSTEM_ID,
SOURCE_TRAN_NO,
SOURCE_TRAN_VER,
EVENT_AUDIT_ID,
BUSINESS_DATE,
AE_ACC_EVENT_TYPE_ID,
AE_SUB_EVENT_ID,
AE_ACCOUNTING_DATE::datetime AE_ACCOUNTING_DATE, -- Movement Date
AE_POS_NEG,
AE_DIMENSION_1, 
substring(AE_DIMENSION_2,1,4) AE_DIMENSION_2, -- Uw Year
substring(AE_DIMENSION_3,1,4) AE_DIMENSION_3, -- Acc Year
co.major as AE_DIMENSION_4, -- Mythic "COB" Nonsense
tmm.targetmarket as AE_DIMENSION_5, -- Target Market
AE_DIMENSION_6, -- Intercompany
AE_DIMENSION_7, -- Placing Basis
AE_DIMENSION_8, -- Contract Type
AE_DIMENSION_9, 
AE_DIMENSION_10,
co.class_code as AE_DIMENSION_11, -- More "COB" Stuff
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
case when change_pol_level = 1 then 'New' Else 'Not New' end as CLIENT_TEXT8, -- New Policy Marker
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

   left join {{ref('are_sample_cobs')}} co on            
      l.contract_clicode = co.eclipse_policy_reference

   left join distinct_targmarkets tmm on 
       tmm.lineref = l.contract_clicode

where lagged_change_marker != change_marker

order by _snapshot_date,_nk_transin