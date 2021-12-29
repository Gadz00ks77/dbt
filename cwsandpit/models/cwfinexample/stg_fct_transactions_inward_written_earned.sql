
with cte_row_snaps as (

select distinct il.policy_reference,fc.parent_category,fc.category,fc.sub_category,te.transaction_event,te.transaction_sub_event,oc.isochar_code as orig_ccy,sc.isochar_code as sett_ccy, ps.*
from {{ref('fct_trans_periodic_snapshot')}} ps
    join {{ref('dim_insured_line')}} il on ps.insured_line_key = il.insured_line_key
    join {{ref('dim_financial_categories')}} fc on ps.financial_category_key = fc.financial_category_key
    join {{ref('dim_transaction_events')}} te on ps.transaction_event_key = te.transaction_event_key
    join {{ref('dim_currencies')}} oc on ps.orig_ccy_key = oc.currency_key
    join {{ref('dim_currencies')}} sc on ps.sett_ccy_key = sc.currency_key
where row_source = 'written'
)



select 
sha2(id) as transaction_key,
'are' as system_source,
'are earned' as event_source,
object_construct(
   '0_event_source','are earned',
   '1_id',o.id
            ) as nk_transaction,
'n/a' as change_key,
null as instalmentnum,
o.is_addition,
null as has_contra,
null as transaction_rate,
'A' as transaction_basis,
o.transaction_date,
ps.financial_category_key,
te.transaction_event_key,
ps.accounting_period_key,
ps.risk_key,
ps.class_of_business_key,
ps.claim_key,
ps.claim_peril_key,
sha2('n/a') as loss_event_key,
ps.insured_line_key,
ps.inception_date_key,
ps.expiry_date_key,
ps.uw_year_key,
o.transaction_date as lossdatefrom_key,
null as lossdateto_key,
ps.assured_party_key,
sha2('n/a') as assured_address_key,
ps.reassured_party_key,
ps.client_party_key,
ps.legal_entity_key,
ps.producing_team_key as producting_team_key,
ps.placer_key,
ps.producer_key,
ps.retailer_key,
ps.orig_ccy_key as orig_currency_key,
ps.sett_ccy_key as sett_currency_key,
o.original_currency_amount as transaction_orig_amt,
o.settlement_currency_amount as transaction_sett_amt

from {{ref('stg_are_contract_transaction_out')}} o
   join cte_row_snaps ps
        on ps.policy_reference = o.eclipse_policy_reference
        and ps.snapshot_date = o.file_send_date
        and ps.parent_category = o.financial_category_tier_2
        and ps.category = o.financial_category_tier_3
        and ps.sub_category = o.financial_category_tier_4
        and ps.transaction_sub_event = o.transaction_sub_event
        and ps.orig_ccy = o.original_currency_iso_code
        and ps.sett_ccy = o.settlement_currency_iso_code

   join dim_transaction_events te 
        on te.transaction_event = 'Inward Written Earned'
        and te.transaction_sub_event = ps.transaction_sub_event

where o.transaction_event = 'Inward Written Earned'