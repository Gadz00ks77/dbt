select 

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
sha2('n/a') as claim_key,
null as date_of_loss_key,
sha2('n/a') as claim_peril_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_written_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = te.transaction_event
    and aem.transaction_sub_event = te.transaction_sub_event
    and ws.parent_category = aem.parent_financial_category
    and ws.category = case when aem.fdu_financial_category = '*' then ws.category else aem.fdu_financial_category end
    and ws.sub_category = case when aem.fdu_sub_financial_category = '*' then ws.sub_category else aem.fdu_sub_financial_category end
    and ws.is_addition = case when aem.fdu_isaddition = '*' then ws.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT THE MOMENT

group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key,
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code

union

select

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
sha2('n/a') as claim_key,
null as date_of_loss_key,
sha2('n/a') as claim_peril_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_booked_nb_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = te.transaction_event
    and aem.transaction_sub_event = te.transaction_sub_event
    and ws.parent_category = aem.parent_financial_category
    and ws.category = case when aem.fdu_financial_category = '*' then ws.category else aem.fdu_financial_category end
    and ws.sub_category = case when aem.fdu_sub_financial_category = '*' then ws.sub_category else aem.fdu_sub_financial_category end
    and ws.is_addition = case when aem.fdu_isaddition = '*' then ws.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT THE MOMENT

group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key,
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code

union 

select 

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
sha2('n/a') as claim_key,
null as date_of_loss_key,
sha2('n/a') as claim_peril_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_ledger_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_booked_b_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = te.transaction_event
    and aem.transaction_sub_event = te.transaction_sub_event
    and ws.parent_category = aem.parent_financial_category
    and ws.category = case when aem.fdu_financial_category = '*' then ws.category else aem.fdu_financial_category end
    and ws.sub_category = case when aem.fdu_sub_financial_category = '*' then ws.sub_category else aem.fdu_sub_financial_category end
    and ws.is_addition = case when aem.fdu_isaddition = '*' then ws.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT THE MOMENT

group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key,
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code


union 

select 

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
sha2('n/a') as claim_key,
null as date_of_loss_key,
sha2('n/a') as claim_peril_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_ledger_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_paid_b_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = te.transaction_event
    and aem.transaction_sub_event = te.transaction_sub_event
    and ws.parent_category = aem.parent_financial_category
    and ws.category = case when aem.fdu_financial_category = '*' then ws.category else aem.fdu_financial_category end
    and ws.sub_category = case when aem.fdu_sub_financial_category = '*' then ws.sub_category else aem.fdu_sub_financial_category end
    and ws.is_addition = case when aem.fdu_isaddition = '*' then ws.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT THE MOMENT

group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key,
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code

union

select 

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
claim_key,
lossdatefrom as date_of_loss_key,
claim_peril_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_claims_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

join {{ref('dim_perils')}} pd
    on ws.claim_peril_key = pd.peril_key

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = te.transaction_event
    and aem.transaction_sub_event = te.transaction_sub_event
    and ws.parent_category = aem.parent_financial_category
    and ws.category = case when aem.fdu_financial_category = '*' then ws.category else aem.fdu_financial_category end
    and ws.sub_category = case when aem.fdu_sub_financial_category = '*' then ws.sub_category else aem.fdu_sub_financial_category end
    and ws.is_addition = case when aem.fdu_isaddition = '*' then ws.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT THE MOMENT

where 
    pd.peril_code = 'GEBL' and aem.fdu_claim_peril = 'GEBL'

group by
ws.claim_key,
ws.claim_peril_key,
ws.lossdatefrom,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key,
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code

union

select 

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
claim_key,
lossdatefrom as date_of_loss_key,
claim_peril_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_claims_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

join {{ref('dim_perils')}} pd
    on ws.claim_peril_key = pd.peril_key

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = te.transaction_event
    and aem.transaction_sub_event = te.transaction_sub_event
    and ws.parent_category = aem.parent_financial_category
    and ws.category = case when aem.fdu_financial_category = '*' then ws.category else aem.fdu_financial_category end
    and ws.sub_category = case when aem.fdu_sub_financial_category = '*' then ws.sub_category else aem.fdu_sub_financial_category end
    and ws.is_addition = case when aem.fdu_isaddition = '*' then ws.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT THE MOMENT

where 
    pd.peril_code != 'GEBL' and aem.fdu_claim_peril in ('!=GEBL','*')

group by

ws.claim_key,
ws.claim_peril_key,
ws.lossdatefrom,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
insured_line_key,
orig_ccy_key,
sett_ccy_key,
inception_date_key,
expiry_date_key,
uw_year_key,
assured_party_key,
reassured_party_key,
client_party_key,
legal_entity_key,
producting_team_key,
placer_key,
producer_key,
retailer_key,
ws.transaction_event_key,
financial_category_key,
is_addition,
aem.accounting_event_code