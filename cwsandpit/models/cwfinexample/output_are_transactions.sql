with fetch_sample as (
select 

ft.transaction_key,
il.policy_reference as eclipse_policy_reference,
cl.claim_reference,
cl.unique_claim_reference,
peril.peril_code,
r.placing_basis,
entity.party_name as legal_entity,
team.party_name as producing_team,
ft.inception_date_key as inception_date,
ft.expiry_date_key as expiry_date,
il.date_written,
il.line_status as status_at_transaction_time,
sc.isochar_code as settlement_currency,
oc.isochar_code as original_currency,
ft.transaction_date,
te.transaction_event,
te.transaction_sub_event,
ft.instalmentnum,
ftc.parent_category,
ftc.category,
ftc.sub_category,
ft.is_addition,
ft.transaction_sett_amt,
ft.transaction_orig_amt
    
  from 
        {{ref('fct_transactions')}} ft
            join {{ref('dim_risk')}} r on 
                ft.risk_key = r.risk_key
            join {{ref('dim_insured_line')}} il on
                ft.insured_line_key = il.insured_line_key
            join {{ref('dim_financial_categories')}} ftc on
                ft.financial_category_key = ftc.financial_category_key
            join {{ref('dim_transaction_events')}} te on
                ft.transaction_event_key = te.transaction_event_key
            join {{ref('dim_currencies')}} sc on
                ft.sett_currency_key = sc.currency_key
            join {{ref('dim_currencies')}} oc on
                ft.orig_currency_key = oc.currency_key
            join {{ref('are_sample')}} are on
                are.sampleset = il.policy_reference
            join {{ref('dim_parties')}} entity on
                ft.legal_entity_key = entity.party_key
            join {{ref('dim_parties')}} team on
                ft.producting_team_key = team.party_key
            join {{ref('dim_claims')}} cl on
                ft.claim_key = cl.claim_key
            join {{ref('dim_perils')}} peril on
                ft.claim_peril_key = peril.peril_key

 where transaction_sett_amt != 0 
 --and eclipse_policy_reference = 'AQ819E21A000'
  )

select distinct 
    fs.*
    ,aem.transaction_event as mapped_matrix_transaction_event
    ,aem.transaction_sub_event as mapped_matrix_transaction_sub_event
    ,aem.parent_financial_category as mapped_matrix_parent_financial_category
    ,aem.fdu_financial_category as mapped_matrix_financial_category
    ,aem.fdu_sub_financial_category as mapped_matrix_financial_sub_category
    ,aem.fdu_isaddition as mapped_matrix_is_addition_marker
    ,aem.fdu_claim_peril as mapped_matrix_claim_peril
    ,aem.accounting_event_code
    ,aem.accounting_event_description
from 
fetch_sample fs

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = fs.transaction_event
    and aem.transaction_sub_event = fs.transaction_sub_event
    and fs.parent_category = aem.parent_financial_category
    and fs.category = case when aem.fdu_financial_category = '*' then fs.category else aem.fdu_financial_category end
    and fs.sub_category = case when aem.fdu_sub_financial_category = '*' then fs.sub_category else aem.fdu_sub_financial_category end
    and fs.is_addition = case when aem.fdu_isaddition = '*' then fs.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT TH MOMENT

where fs.transaction_sub_event not in ('Outstanding - Market Movement','Claim Paid - Market') -- These are just not necessary at all.
and fs.peril_code = 'GEBL' and aem.fdu_claim_peril = 'GEBL'

union all

select distinct 
    fs.*
    ,aem.transaction_event as mapped_matrix_transaction_event
    ,aem.transaction_sub_event as mapped_matrix_transaction_sub_event
    ,aem.parent_financial_category as mapped_matrix_parent_financial_category
    ,aem.fdu_financial_category as mapped_matrix_financial_category
    ,aem.fdu_sub_financial_category as mapped_matrix_financial_sub_category
    ,aem.fdu_isaddition as mapped_matrix_is_addition_marker
    ,aem.fdu_claim_peril as mapped_matrix_claim_peril
    ,aem.accounting_event_code
    ,aem.accounting_event_description
from 
fetch_sample fs

left join {{ref('accounting_event_matrix')}} aem
    on aem.transaction_event = fs.transaction_event
    and aem.transaction_sub_event = fs.transaction_sub_event
    and fs.parent_category = aem.parent_financial_category
    and fs.category = case when aem.fdu_financial_category = '*' then fs.category else aem.fdu_financial_category end
    and fs.sub_category = case when aem.fdu_sub_financial_category = '*' then fs.sub_category else aem.fdu_sub_financial_category end
    and fs.is_addition = case when aem.fdu_isaddition = '*' then fs.is_addition else aem.fdu_isaddition end
    and aem.fdu_new_policy in ('Not New','*') -- HARD CONSTRAINT AT TH MOMENT

where fs.transaction_sub_event not in ('Outstanding - Market Movement','Claim Paid - Market') -- These are just not necessary at all.
and fs.peril_code != 'GEBL' and aem.fdu_claim_peril in ('!=GEBL','*')

   