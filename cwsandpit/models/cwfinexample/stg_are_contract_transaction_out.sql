
select

id,
stn_contract_transaction_in_id,
lpg_id,
event_status
remitting_system_id,
to_date(arrival_time,'DD-MON-YYYY HH24:MI:SS') as arrival_time,
source_tran_ver,
to_date(business_date,'DD-MON-YYYY HH24:MI:SS') as file_send_date,
to_date(ae_accounting_date,'DD-MON-YYYY HH24:MI:SS') transaction_date,
ae_pos_neg,
ae_dimension_1 as accounting_principle,
ae_dimension_2 as year_of_account,
ae_dimension_3 as acc_year,
ae_dimension_4 as cob_tier_1_code,
ae_dimension_5 as target_market,
ae_dimension_6 as intercompany,
ae_dimension_7 as placing_basis,
ae_dimension_8 as contract_type,
ae_dimension_11 as cob_tier_3_code,
ae_dimension_12 as eclipse_policy_reference,
contract_clicode as eclipse_policy_reference2,
pl_party_legal_clicode as oracle_gl_entity_code,
cu_currency_iso_code as settlement_currency_iso_code,
other_date1 as accounting_period_month_date,
client_text1 as transaction_event,
client_text1_original as transaction_event_original,
client_text2 as transaction_sub_event,
client_text3 as financial_category_tier_2,
client_text4 as financial_category_tier_3,
client_text5 as financial_category_tier_4,
client_text6 as is_addition,
client_text9 as original_currency_iso_code,
client_text11 as period_basis,
client_text20 as line_status,
client_amount1 as settlement_currency_amount,
client_amount2 as original_currency_amount,
to_date(translation_date,'DD-MON-YYYY HH24:MI:SS') as translation_date,
data_load_id,
file_id,
file_name,
policy_version_number,
to_date(posting_date,'DD-MON-YYYY HH24:MI:SS') as posting_date
from
stn_contract_transaction_out