select 

sha2(
    source_system||
    source||
    measure||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
source_system,
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
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
l.measure,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett,
sum(cumu_transaction_orig_amt_add) as measure_orig_additional, --  not resolved additional summing logic to my satisfaction
sum(cumu_transaction_sett_amt_add) as measure_sett_additional --  ditto


from {{ref('stg_fct_periodic_written_snapshot')}} ws
    join {{ref('stg_measure_lookup')}} l
        on ws.measure_source = l.source
        and ws.parent_category = l.parent_category
        and ws.category = l.category
        and ifnull(ws.sub_category,'n/a') = ifnull(l.sub_category,'n/a')
        and ifnull(ws.is_addition,'N') = ifnull(l.is_addition,'N') 

group by
source_system,
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
l.measure,
l.source

union 

select 

sha2(
    source_system||
    source||
    measure||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
source_system,
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
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
l.measure,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_sett_amt) as measure_sett,
sum(cumu_transaction_orig_amt_add) as measure_orig_additional, --  not resolved additional summing logic to my satisfaction
sum(cumu_transaction_sett_amt_add) as measure_sett_additional --  ditto


from {{ref('stg_fct_periodic_booked_nb_snapshot')}} ws
    join {{ref('stg_measure_lookup')}} l
        on ws.measure_source = l.source
        and ws.parent_category = l.parent_category
        and ws.category = l.category
        and ifnull(ws.sub_category,'n/a') = ifnull(l.sub_category,'n/a')
        and ifnull(ws.is_addition,'N') = ifnull(l.is_addition,'N') 

group by
source_system,
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
l.measure,
l.source

union 

select 

sha2(
    source_system||
    source||
    measure||
    risk_key||
    insured_line_key||
    orig_ccy_key||
    sett_ccy_key||
    snapshot_date::text
    ) as periodic_key,
source_system,
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
producting_team_key as producing_team_key, --idiot
placer_key,
producer_key,
retailer_key,
l.measure,
sum(cumu_transaction_orig_amt) as measure_orig,
sum(cumu_transaction_ledger_amt) as measure_sett,
sum(cumu_transaction_orig_amt_add) as measure_orig_additional, --  not resolved additional summing logic to my satisfaction
sum(cumu_transaction_ledger_amt_add) as measure_sett_additional --  ditto


from {{ref('stg_fct_periodic_booked_b_snapshot')}} ws
    join {{ref('stg_measure_lookup')}} l
        on ws.measure_source = l.source
        --and ws.parent_category = l.parent_category
        --and ws.category = l.category
        and ifnull(ws.sub_category,'n/a') = ifnull(l.sub_category,'n/a')
        and ifnull(ws.is_addition,'N') = ifnull(l.is_addition,'N') 

group by
source_system,
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
l.measure,
l.source
