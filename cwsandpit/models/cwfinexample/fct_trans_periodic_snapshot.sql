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
'written' as row_source,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
--aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_written_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join dim_class_of_business cob 
        on ws.class4 = cob.tier_1_code
        and ws.class1 = cob.tier_2_code
        and ws.class3 = cob.tier_3_code
        and ws.snapshot_date between cob.valid_from and cob.valid_to

group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
is_addition
--aem.accounting_event_code

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
'booked nb' as row_source,    
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
--aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_booked_nb_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join dim_class_of_business cob 
        on ws.class4 = cob.tier_1_code
        and ws.class1 = cob.tier_2_code
        and ws.class3 = cob.tier_3_code
        and ws.snapshot_date between cob.valid_from and cob.valid_to

group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
is_addition
--aem.accounting_event_code

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
'booked b' as row_source,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
--aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_ledger_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_booked_b_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join dim_class_of_business cob 
        on ws.class4 = cob.tier_1_code
        and ws.class1 = cob.tier_2_code
        and ws.class3 = cob.tier_3_code
        and ws.snapshot_date between cob.valid_from and cob.valid_to


group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
is_addition
--aem.accounting_event_code


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
'paid b' as row_source,    
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
--aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_ledger_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_paid_b_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

left join dim_class_of_business cob 
        on ws.class4 = cob.tier_1_code
        and ws.class1 = cob.tier_2_code
        and ws.class3 = cob.tier_3_code
        and ws.snapshot_date between cob.valid_from and cob.valid_to


group by
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
is_addition
--aem.accounting_event_code

union

select 

sha2(
    ws.source_system||
    ws.transaction_event_key||
    financial_category_key||
    is_addition||
    risk_key||
    claim_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
'claims gebl' as row_source,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
--aem.accounting_event_code,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett

from {{ref('stg_fct_trans_periodic_claims_snapshot')}} ws

join {{ref('dim_transaction_events')}} te
    on te.transaction_event_key = ws.transaction_event_key

join {{ref('dim_perils')}} pd
    on ws.claim_peril_key = pd.peril_key

left join dim_class_of_business cob 
        on ws.class4 = cob.tier_1_code
        and ws.class1 = cob.tier_2_code
        and ws.class3 = cob.tier_3_code
        and ws.snapshot_date between cob.valid_from and cob.valid_to


-- where 
--     pd.peril_code = 'GEBL' --and aem.fdu_claim_peril = 'GEBL'

group by
ws.claim_key,
ws.claim_peril_key,
ws.lossdatefrom,
ws.source_system,
accounting_period_key,
snapshot_date,
risk_key,
cob.class_of_business_key,
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
is_addition
--aem.accounting_event_code

-- union

-- select 

-- sha2(
--     ws.source_system||
--     ws.transaction_event_key||
--     financial_category_key||
--     is_addition||
--     risk_key||
--     insured_line_key||
--     orig_ccy_key||
--     sett_ccy_key||
--     snapshot_date::text
--     ) as periodic_key,
-- 'claims not gebl' as row_source,
-- ws.source_system,
-- accounting_period_key,
-- snapshot_date,
-- risk_key,
-- cob.class_of_business_key,
-- insured_line_key,
-- orig_ccy_key,
-- sett_ccy_key,
-- inception_date_key,
-- expiry_date_key,
-- uw_year_key,
-- claim_key,
-- lossdatefrom as date_of_loss_key,
-- claim_peril_key,
-- assured_party_key,
-- reassured_party_key,
-- client_party_key,
-- legal_entity_key,
-- producting_team_key as producing_team_key, --idiot
-- placer_key,
-- producer_key,
-- retailer_key,
-- ws.transaction_event_key,
-- financial_category_key,
-- is_addition,
-- --aem.accounting_event_code,
-- sum(cumu_transaction_orig_amt) as measure_orig,
-- sum(cumu_transaction_sett_amt) as measure_sett

-- from {{ref('stg_fct_trans_periodic_claims_snapshot')}} ws

-- join {{ref('dim_transaction_events')}} te
--     on te.transaction_event_key = ws.transaction_event_key

-- join {{ref('dim_perils')}} pd
--     on ws.claim_peril_key = pd.peril_key

-- left join dim_class_of_business cob 
--         on ws.class4 = cob.tier_1_code
--         and ws.class1 = cob.tier_2_code
--         and ws.class3 = cob.tier_3_code
--         and ws.snapshot_date between cob.valid_from and cob.valid_to


-- where 
--     pd.peril_code != 'GEBL' --and aem.fdu_claim_peril in ('!=GEBL','*')

-- group by

-- ws.claim_key,
-- ws.claim_peril_key,
-- ws.lossdatefrom,
-- ws.source_system,
-- accounting_period_key,
-- snapshot_date,
-- risk_key,
-- cob.class_of_business_key,
-- insured_line_key,
-- orig_ccy_key,
-- sett_ccy_key,
-- inception_date_key,
-- expiry_date_key,
-- uw_year_key,
-- assured_party_key,
-- reassured_party_key,
-- client_party_key,
-- legal_entity_key,
-- producting_team_key,
-- placer_key,
-- producer_key,
-- retailer_key,
-- ws.transaction_event_key,
-- financial_category_key,
-- is_addition
--aem.accounting_event_code