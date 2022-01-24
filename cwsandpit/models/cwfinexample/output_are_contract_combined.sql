

with cte_1 as (
select 'FDU' as source, *
from 
{{ref('output_are_contract_in')}}
-- union all 
-- select 'Pipeline' as source,*
-- from
-- {{ref('output_are_manual_contract_pipeline')}}
-- union all 
-- select 'RIPEstimates' as source, *
-- from
-- {{ref('output_are_manual_contract_ripestimates')}} 
),
pref as (
select 'FDU' as source, 1 as pref
union all select 'Pipeline' as source, 3 as pref
union all select 'RIPEstimates' as source, 2 as pref
)
,
fields as (
select 

_snapshot_date,
'n/a' as _NK_FINAL_TRANSIN,
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
row_number() over (partition by contract_number order by _snapshot_date) CONTRACT_VERSION_NO,
_snapshot_date as CONTRACT_VERSION_DATE,
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
FEE_AMOUNT,
p.pref as Preference,
min(p.pref) over (partition by contract_clicode order by p.pref) as BestPref

from cte_1 c

    left join pref p
        on c.source = p.source

)


select * 
from fields 
where Preference = BestPref