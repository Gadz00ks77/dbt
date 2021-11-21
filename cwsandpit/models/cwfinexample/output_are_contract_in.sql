
with cte_informat as (

select

null as ID,
null as LPG_ID,
null as EVENT_STATUS,
null as EVENT_ERROR_STRING,
null as NO_RETRIES,
null as STAN_RULE_IDENT,
null as SOURCE_SYS_INST_CODE,
null as STATIC_SYS_INST_CODE,
null as CONTRACT_SYS_INST_CODE,
null as ACTIVE,
null as INPUT_BY,
null as INPUT_TIME,
null as PROCESS_ID,
null as SUB_SYSTEM_ID,
null as MESSAGE_ID,
null as REMITTING_SYSTEM_ID,
null as ARRIVAL_TIME,
il.policy_reference as CONTRACT_CLICODE,
'DEFAULT' as PRODUCT_CLICODE,
null as BO_BOOK_CLICODE,
null as DESCRIPTION,
il.policy_reference as CONTRACT_NUMBER,
null as CONTRACT_VERSION_NO,
null as CONTRACT_VERSION_DATE,
null as CONTRACT_STATUS,
null as ISSUE_DATE,
null as PAID_DATE,
inception_date_key as START_DATE,
expiry_date_key as END_DATE,
null as MOVEMENT_TYPE,
null as CH_CHANNEL_CLICODE,
null as CU_CURRENCY_ISO_CODE,
null as CONTRACT_TYPE,
null as CESSION_PERCENT,
null as PREMIUM_PERCENT,
null as CLAIM_CAP_AMOUNT,
null as NF_OPTION_FLAG,
null as DIV_OPTION_FLAG,
null as COVER_NOTE_DATE,
null as COVER_NOTE_DESCRIPTION,
null as COVER_NOTE_START_DATE,
null as COVER_NOTE_END_DATE,
null as COVER_NOTE_SIGNATURE_DATE,
null as COVER_NOTE_SIGNED_BY,
null as COVER_NOTE_SIGNED_FLAG,
null as JURISDICTION,
null as SIGNATURE_DATE,
null as INDEMNITY_AMOUNT,
null as BENEFIT_LIMIT,
null as PREMIUM_AMOUNT,
null as PREMIUM_FREQUENCY_CLICODE,
null as PREMIUM_TERM,
null as CONTRACT_FEE,
r.placing_basis as CLIENT_TEXT1,
null as CLIENT_TEXT2,
r.period_basis as CLIENT_TEXT3,
producing.party_name as CLIENT_TEXT4,
null as CLIENT_TEXT5,
null as CLIENT_TEXT6,
null as CLIENT_TEXT7,
null as CLIENT_TEXT8,
null as CLIENT_TEXT9,
null as CLIENT_TEXT10,
null as CLIENT_TEXT11,
null as CLIENT_TEXT12,
null as CLIENT_TEXT13,
null as CLIENT_TEXT14,
null as CLIENT_TEXT15,
null as CLIENT_TEXT16,
null as CLIENT_TEXT17,
null as CLIENT_TEXT18,
null as CLIENT_TEXT19,
null as CLIENT_TEXT20,
null as CLIENT_AMOUNT1,
null as CLIENT_AMOUNT2,
null as CLIENT_AMOUNT3,
null as CLIENT_AMOUNT4,
null as CLIENT_AMOUNT5,
null as CLIENT_AMOUNT6,
null as CLIENT_AMOUNT7,
null as CLIENT_AMOUNT8,
null as CLIENT_AMOUNT9,
null as CLIENT_AMOUNT10,
null as CLIENT_DATE1,
null as CLIENT_DATE2,
null as CLIENT_DATE3,
null as CLIENT_DATE4,
null as CLIENT_DATE5,
null as CLIENT_DATE6,
null as CLIENT_DATE7,
null as CLIENT_DATE8,
null as CLIENT_DATE9,
null as CLIENT_DATE10,
null as REINSURANCE_FLAG,
null as CESSION_AMOUNT,
null as NET_LOSS_RETENTION,
null as RETENTION_REASON,
null as CAP_AMOUNT,
null as CAP_PERCENT,
null as LOSS_OCCURRANCE,
null as CLAIM_AGG_FREQUENCY_CLICODE,
null as EXP_LOSS_RATIO,
null as DOUBLE_INSURANCE_FLAG,
null as RETROCESSION_FLAG,
null as FEE_PERCENT,
null as FEE_AMOUNT,
ws.snapshot_date ,
r.placing_basis         as CDC_1, -- Placing Basis
r.contract_type         as CDC_2, -- Contract Type
il.policy_reference     as NK_1, -- Insured Line Eclipse Policy Reference
le.party_name           as CDC_3, -- Legal Entity
te.transaction_event        as NK_2, -- Transaction Event
te.transaction_sub_event    as NK_3, -- Transaction Sub Event
fc.parent_category          as NK_4, -- Financial Parent Category
fc.category                 as NK_5, -- Financial Category
fc.sub_category             as NK_6, -- Financial Sub Category
ws.is_addition              as NK_7, -- Is Addition Marker
pd.peril_code               as NK_8, -- Peril Code (N/A for Premium)
'TBC'                       as NK_9, -- New Policy Marker (To Be Evaluated Later)
oc.isochar_code             as NK_10,
sc.isochar_code             as NK_11,
'TBC'                       as CDC_4,
il.line_status              as CDC_5, -- Insured Line Status
sum(ws.measure_sett)        as CDC_6, -- Settlement Currency Snapshot Amount
sum(ws.measure_orig)        as CDC_7 -- Original Currency Snapshot Amount

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

    join {{ref('dim_parties')}} producing
        on ws.producing_team_key = producing.party_key

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

    where il.policy_reference in (
                'DA262H21A000',
                'DB174V21A000',
                'AD952Z20A050',
                'AA052F21B000',
                'AK719Z20A001'
    )

--    join {{ref('are_sample')}} are_sample
--        on il.policy_reference = are_sample.sampleset  -- LIMITING EVERYTHING TO THE SAMPLE DEFINED BY SS. CAN BE REMOVED LATER FOR ALL POLICIES.

group by 

    ws.source_system,
    ws.snapshot_date,
    ws.uw_year_key,
    r.placing_basis,
    r.contract_type,
    r.period_basis,
    il.policy_reference,
    le.party_name,
    ap.accounting_period,
    te.transaction_event,
    te.transaction_sub_event,
    fc.parent_category,
    fc.category,
    fc.sub_category,
    ws.is_addition,
    pd.peril_code,
    il.line_status,
    ws.inception_date_key,
    ws.expiry_date_key,
    producing.party_name,
    oc.isochar_code,
    sc.isochar_code

), cte_capture_change as (
select 
    NK_1
    -- ||'|'||NK_2||'|'||
    -- NK_3||'|'||
    -- NK_4||'|'||
    -- NK_5||'|'||
    -- NK_6||'|'||
    -- NK_7||'|'||
    -- NK_8||'|'||
    -- NK_9                    
    as nk_final_transin,
    NK_1
    ||'|'||NK_2||'|'||
    NK_3||'|'||
    NK_4||'|'||
    NK_5||'|'||
    NK_6||'|'||
    NK_7||'|'||
    NK_8||'|'||
    NK_9||'|'||
    NK_10||'|'||
    NK_11                    
    as nk_transin
    ,conditional_change_event(
            CDC_1||'|'||
            CDC_2||'|'||
            CDC_3||'|'||
            CDC_4||'|'||
            CDC_5||'|'||
            CDC_6::text||'|'||
            CDC_7::text
        ) over
        (
        partition by 
            NK_1||'|'||
            NK_2||'|'||
            NK_3||'|'||
            NK_4||'|'||
            NK_5||'|'||
            NK_6||'|'||
            NK_7||'|'||
            NK_8||'|'||
            NK_9||'|'||
            NK_10||'|'||
            NK_11
        order by
          snapshot_date

        )                           as change_marker
    ,i.*

from cte_informat i

    join {{ref('dim_date')}} ap 
        on snapshot_date::text = ap.date::text

--where 
        --ap.day_of_month = 1 -- this is removed as I want to produce one "specific date" snapshot
        

),

cte_lagged as (

select ifnull(lag(change_marker) over (partition by nk_transin order by snapshot_date),-1) as lagged_change_marker, * 
from cte_capture_change
  )
  
select distinct

nk_final_transin as _nk_final_transin,
snapshot_date as _snapshot_date,
EVENT_ERROR_STRING,
NO_RETRIES,
STAN_RULE_IDENT,
PROCESS_ID,
SUB_SYSTEM_ID,
MESSAGE_ID,
REMITTING_SYSTEM_ID,
CONTRACT_CLICODE,
PRODUCT_CLICODE,
BO_BOOK_CLICODE,
DESCRIPTION,
CONTRACT_NUMBER,
CONTRACT_VERSION_NO,
CONTRACT_VERSION_DATE,
CONTRACT_STATUS,
ISSUE_DATE,
PAID_DATE,
START_DATE::datetime as START_DATE,
END_DATE::datetime as END_DATE,
MOVEMENT_TYPE,
CH_CHANNEL_CLICODE,
CU_CURRENCY_ISO_CODE,
CONTRACT_TYPE,
CESSION_PERCENT,
PREMIUM_PERCENT,
CLAIM_CAP_AMOUNT,
NF_OPTION_FLAG,
DIV_OPTION_FLAG,
COVER_NOTE_DATE,
COVER_NOTE_DESCRIPTION,
COVER_NOTE_START_DATE,
COVER_NOTE_END_DATE,
COVER_NOTE_SIGNATURE_DATE,
COVER_NOTE_SIGNED_BY,
COVER_NOTE_SIGNED_FLAG,
JURISDICTION,
SIGNATURE_DATE,
INDEMNITY_AMOUNT,
BENEFIT_LIMIT,
PREMIUM_AMOUNT,
PREMIUM_FREQUENCY_CLICODE,
PREMIUM_TERM,
CONTRACT_FEE,
CLIENT_TEXT1,
CLIENT_TEXT2,
CLIENT_TEXT3,
CLIENT_TEXT4,
CLIENT_TEXT5,
CLIENT_TEXT6,
CLIENT_TEXT7,
CLIENT_TEXT8,
CLIENT_TEXT9,
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
CLIENT_TEXT20,
CLIENT_AMOUNT1,
CLIENT_AMOUNT2,
CLIENT_AMOUNT3,
CLIENT_AMOUNT4,
CLIENT_AMOUNT5,
CLIENT_AMOUNT6,
CLIENT_AMOUNT7,
CLIENT_AMOUNT8,
CLIENT_AMOUNT9,
CLIENT_AMOUNT10,
CLIENT_DATE1,
CLIENT_DATE2,
CLIENT_DATE3,
CLIENT_DATE4,
CLIENT_DATE5,
CLIENT_DATE6,
CLIENT_DATE7,
CLIENT_DATE8,
CLIENT_DATE9,
CLIENT_DATE10,
REINSURANCE_FLAG,
CESSION_AMOUNT,
NET_LOSS_RETENTION,
RETENTION_REASON,
CAP_AMOUNT,
CAP_PERCENT,
LOSS_OCCURRANCE,
CLAIM_AGG_FREQUENCY_CLICODE,
EXP_LOSS_RATIO,
DOUBLE_INSURANCE_FLAG,
RETROCESSION_FLAG,
FEE_PERCENT,
FEE_AMOUNT

from cte_lagged
where lagged_change_marker != change_marker
order by _snapshot_date,_nk_final_transin