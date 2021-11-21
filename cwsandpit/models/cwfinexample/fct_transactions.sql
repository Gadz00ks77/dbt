select 

tr.transaction_key,
tr.system_source,
tr.event_source,
tr.nk_transaction,
tr.change_key,
tr.instalmentnum::text as instalmentnum,
tr.is_addition,
'n/a' as has_contra,
tr.amtpct as transaction_rate,
tr.deductionind as transaction_basis,
tr.transaction_date::date as transaction_date,
categories.financial_category_key,
t_events.transaction_event_key,
ap.accounting_period_key,
r.risk_key,
cob.class_of_business_key,
sha2('n/a') as claim_key,
sha2('n/a') as claim_peril_key,
sha2('n/a') as loss_event_key,
li.insured_line_key,
tr.inceptiondate::date as inception_date_key,
tr.expirydate::date as expiry_date_key,
(tr.yoa||'-01'||'-01') as uw_year_key,
null as lossdatefrom_key,
null as lossdateto_key,
ifnull(pa.party_key,sha2('Unknown')) as assured_party_key,
ifnull(addr_ass.address_key,sha2('Unknown')) as assured_address_key,
ifnull(pr.party_key,sha2('Unknown')) as reassured_party_key,
ifnull(pc.party_key,sha2('Unknown')) as client_party_key,
ifnull(pe.party_key,sha2('Unknown')) as legal_entity_key,
ifnull(pt.party_key,sha2('Unknown')) as producting_team_key,
ifnull(ppl.party_key,sha2('Unknown')) as placer_key,
ifnull(ppr.party_key,sha2('Unknown')) as producer_key,
ifnull(pre.party_key,sha2('Unknown')) as retailer_key,
orig_ccy.currency_key as orig_currency_key,
sett_ccy.currency_key as sett_currency_key,
transaction_orig_gross_amt as transaction_orig_amt,
transaction_sett_gross_amt as transaction_sett_amt

from {{ref('stg_fct_written_transactions')}} tr

    left join {{ref('dim_financial_categories')}} categories
        on tr.financial_sub_category::text = categories.nk_financial_category
        
    left join {{ref('stg_written_event_lookup')}} lkp 
        on tr.instalmenttype = lkp.lookup_value
        
    left join {{ref('dim_transaction_events')}} t_events
        on lkp.transaction_event = t_events.transaction_event
        and lkp.transaction_sub_event = t_events.transaction_sub_event

    left join dim_class_of_business cob 
        on tr.class4 = cob.tier_1_code
        and tr.class1 = cob.tier_2_code
        and tr.class3 = cob.tier_3_code
        and tr.transaction_date between cob.valid_from and cob.valid_to
        
    left join {{ref('dim_risk')}} r
        on tr.policyid = r.risk_nk
        and r.source_system = tr.system_source --stupid
        and tr.transaction_date between r.effective_from and r.effective_to 
    
    left join {{ref('dim_insured_line')}} li
        on tr.policylineid::text = li.insured_line_nk
        and r.source_system = tr.system_source --i know, i know
        and tr.transaction_date between li.effective_from and li.effective_to

    left join {{ref('dim_parties')}} pa
        on tr.assured::text = pa.party_nk
        and pa.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pa.effective_from and pa.effective_to

    left join {{ref('dim_addresses')}} addr_ass
        on tr.assured_addr_id::text = addr_ass.address_nk
        and addr_ass.source_system = tr.system_source --i know, i know
        and tr.transaction_date between addr_ass.effective_from and addr_ass.effective_to

    left join {{ref('dim_parties')}} pr
        on tr.reassured::text = pr.party_nk
        and pr.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pr.effective_from and pr.effective_to

    left join {{ref('dim_parties')}} pc
        on tr.client::text = pc.party_nk
        and pc.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pc.effective_from and pc.effective_to

    left join {{ref('dim_parties')}}  pe
        on tr.synd::text = pe.party_nk
        and pe.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pe.effective_from and pe.effective_to

    left join {{ref('dim_parties')}}  pt
        on tr.producingteam::text = pt.party_nk
        and pt.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pt.effective_from and pt.effective_to
        
    left join {{ref('dim_parties')}}  ppl 
        on tr.placer::text||'broker' = ppl.party_nk
        and ppl.source_system = tr.system_source
        and tr.transaction_date between ppl.effective_from and ppl.effective_to
        
    left join {{ref('dim_parties')}}  ppr 
        on tr.producer::text||'broker' = ppr.party_nk
        and ppr.source_system = tr.system_source
        and tr.transaction_date between ppr.effective_from and ppr.effective_to

    left join {{ref('dim_parties')}}  pre 
        on tr.retailer::text||'broker' = pre.party_nk
        and pre.source_system = tr.system_source
        and tr.transaction_date between pre.effective_from and pre.effective_to

    left join dim_accounting_periods ap  
        on tr.transaction_date::date::text = ap.date

    left join {{ref('dim_currencies') }} orig_ccy  
        on tr.origccyiso = orig_ccy.isochar_code
        and tr.transaction_date between orig_ccy.effective_from and orig_ccy.effective_to

    left join {{ref('dim_currencies') }} sett_ccy  
        on tr.settccyiso = sett_ccy.isochar_code
        and tr.transaction_date between sett_ccy.effective_from and sett_ccy.effective_to

union

select 

tr.transaction_key,
tr.system_source,
tr.event_source,
tr.nk_transaction,
tr.change_key,
'n/a' as instalmentnum,
'n/a' as is_addition,
'n/a' as has_contra,
null  as transaction_rate,
'n/a' as transaction_basis,
tr.movementdateonly as transaction_date,
categories.financial_category_key,
t_events.transaction_event_key,
ap.accounting_period_key,
r.risk_key,
cob.class_of_business_key,
c.claim_key,
ifnull(pd.peril_key,sha2('Unknown')) as claim_peril_key,
le.loss_event_key,
li.insured_line_key,
tr.inceptiondate::date as inception_date_key,
tr.expirydate::date as expiry_date_key,
(tr.yoa||'-01'||'-01') as uw_year_key,
tr.lossdatefrom as lossdatefrom_key,
tr.lossdateto as lossdateto_key,
ifnull(pa.party_key,sha2('Unknown')) as assured_party_key,
ifnull(addr_ass.address_key,sha2('Unknown')) as assured_address_key,
ifnull(pr.party_key,sha2('Unknown')) as reassured_party_key,
ifnull(pc.party_key,sha2('Unknown')) as client_party_key,
ifnull(pe.party_key,sha2('Unknown')) as legal_entity_key,
ifnull(pt.party_key,sha2('Unknown')) as producting_team_key,
ifnull(ppl.party_key,sha2('Unknown')) as placer_key,
ifnull(ppr.party_key,sha2('Unknown')) as producer_key,
ifnull(pre.party_key,sha2('Unknown')) as retailer_key,
orig_ccy.currency_key as orig_currency_key,
sett_ccy.currency_key as sett_currency_key,
transaction_orig_gross_amt as transaction_orig_amt,
transaction_sett_gross_amt as transaction_sett_amt

from {{ref('stg_fct_transactions_claims')}} tr

    left join {{ref('dim_financial_categories')}} categories
        on tr.Category_Description::text = categories.sub_category
        and categories.parent_category = 'Claims'

    left join dim_class_of_business cob 
        on tr.class4 = cob.tier_1_code
        and tr.class1 = cob.tier_2_code
        and tr.class3 = cob.tier_3_code
        and tr.movementdateonly between cob.valid_from and cob.valid_to

    left join {{ref('dim_transaction_events')}} t_events
        on tr.transaction_event = t_events.transaction_event
        and tr.transaction_subevent = t_events.transaction_sub_event

    left join {{ref('dim_perils')}} pd 
        on tr.claimperil = pd.peril_nk
        
    left join {{ref('dim_risk')}} r
        on tr.policyid = r.risk_nk
        and r.source_system = tr.system_source --stupid
        and tr.movementdateonly between r.effective_from and r.effective_to 

    left join {{ref('dim_claims')}} c
        on tr.claimid::text = c.claim_nk
        and c.source_system = tr.system_source --stupid
        and tr.movementdateonly between c.effective_from and c.effective_to 

    left join {{ref('dim_loss_events')}} le
        on tr.claimeventid::text = le.loss_event_nk
        and le.source_system = tr.system_source --stupid
        and tr.movementdateonly between le.effective_from and le.effective_to

    left join {{ref('dim_insured_line')}} li
        on tr.policylineid::text = li.insured_line_nk
        and r.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between li.effective_from and li.effective_to

    left join {{ref('dim_parties')}} pa
        on tr.assured::text = pa.party_nk
        and pa.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between pa.effective_from and pa.effective_to

    left join {{ref('dim_addresses')}} addr_ass
        on tr.assured_address::text = addr_ass.address_nk
        and addr_ass.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between addr_ass.effective_from and addr_ass.effective_to

    left join {{ref('dim_parties')}} pr
        on tr.reassured::text = pr.party_nk
        and pr.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between pr.effective_from and pr.effective_to

    left join {{ref('dim_parties')}} pc
        on tr.client::text = pc.party_nk
        and pc.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between pc.effective_from and pc.effective_to

    left join {{ref('dim_parties')}}  pe
        on tr.synd::text = pe.party_nk
        and pe.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between pe.effective_from and pe.effective_to

    left join {{ref('dim_parties')}}  pt
        on tr.producingteam::text = pt.party_nk
        and pt.source_system = tr.system_source --i know, i know
        and tr.movementdateonly between pt.effective_from and pt.effective_to
        
    left join {{ref('dim_parties')}}  ppl 
        on tr.placer::text||'broker' = ppl.party_nk
        and ppl.source_system = tr.system_source
        and tr.movementdateonly between ppl.effective_from and ppl.effective_to
        
    left join {{ref('dim_parties')}}  ppr 
        on tr.producer::text||'broker' = ppr.party_nk
        and ppr.source_system = tr.system_source
        and tr.movementdateonly between ppr.effective_from and ppr.effective_to

    left join {{ref('dim_parties')}}  pre 
        on tr.retailer::text||'broker' = pre.party_nk
        and pre.source_system = tr.system_source
        and tr.movementdateonly between pre.effective_from and pre.effective_to

    left join dim_accounting_periods ap  
        on tr.movementdateonly::text = ap.date

    left join {{ref('dim_currencies') }} orig_ccy  
        on tr.origccyiso = orig_ccy.isochar_code
        and tr.movementdateonly between orig_ccy.effective_from and orig_ccy.effective_to

    left join {{ref('dim_currencies') }} sett_ccy  
        on tr.settccyiso = sett_ccy.isochar_code
        and tr.movementdateonly  between sett_ccy.effective_from and sett_ccy.effective_to


union

select 

tr.transaction_key,
tr.system_source,
tr.event_source,
tr.nk_transaction,
tr.change_key,
tr.instalmentnum::text as instalmentnum,
tr.is_addition,
tr.contraind as has_contra,
tr.amtpct as transaction_rate,
tr.deductionind as transaction_basis,
tr.transaction_date,
categories.financial_category_key,
t_events.transaction_event_key,
ap.accounting_period_key,
r.risk_key,
cob.class_of_business_key,
sha2('n/a') as claim_key,
sha2('n/a') as claim_peril_key,
sha2('n/a') as loss_event_key,
li.insured_line_key,
tr.inceptiondate::date as inception_date_key,
tr.expirydate::date as expiry_date_key,
(tr.yoa||'-01'||'-01') as uw_year_key,
null as lossdatefrom_key,
null as lossdateto_key,
ifnull(pa.party_key,sha2('Unknown')) as assured_party_key,
ifnull(addr_ass.address_key,sha2('Unknown')) as assured_address_key,
ifnull(pr.party_key,sha2('Unknown')) as reassured_party_key,
ifnull(pc.party_key,sha2('Unknown')) as client_party_key,
ifnull(pe.party_key,sha2('Unknown')) as legal_entity_key,
ifnull(pt.party_key,sha2('Unknown')) as producting_team_key,
ifnull(ppl.party_key,sha2('Unknown')) as placer_key,
ifnull(ppr.party_key,sha2('Unknown')) as producer_key,
ifnull(pre.party_key,sha2('Unknown')) as retailer_key,
orig_ccy.currency_key as orig_currency_key,
sett_ccy.currency_key as sett_currency_key,
transaction_orig_amt as transaction_orig_amt,
transaction_sett_amt as transaction_sett_amt

from {{ref('stg_fct_booked_nb_transactions')}} tr

    left join {{ref('dim_financial_categories')}} categories
        on tr.financial_sub_category::text = categories.nk_financial_category
        
    left join {{ref('stg_booked_event_lookup')}} lkp 
        on tr.instalmenttype = lkp.lookup_value
        
    left join {{ref('dim_transaction_events')}} t_events
        on lkp.transaction_event = t_events.transaction_event
        and lkp.transaction_sub_event = t_events.transaction_sub_event

    left join dim_class_of_business cob 
        on tr.class4 = cob.tier_1_code
        and tr.class1 = cob.tier_2_code
        and tr.class3 = cob.tier_3_code
        and tr.transaction_date between cob.valid_from and cob.valid_to
    
    left join {{ref('dim_risk')}} r
        on tr.policyid = r.risk_nk
        and r.source_system = tr.system_source --stupid
        and tr.transaction_date between r.effective_from and r.effective_to 
    
    left join {{ref('dim_insured_line')}} li
        on tr.policylineid::text = li.insured_line_nk
        and r.source_system = tr.system_source --i know, i know
        and tr.transaction_date between li.effective_from and li.effective_to

    left join {{ref('dim_parties')}} pa
        on tr.assured::text = pa.party_nk
        and pa.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pa.effective_from and pa.effective_to

    left join {{ref('dim_addresses')}} addr_ass
        on tr.assured_addr_id::text = addr_ass.address_nk
        and addr_ass.source_system = tr.system_source --i know, i know
        and tr.transaction_date between addr_ass.effective_from and addr_ass.effective_to

    left join {{ref('dim_parties')}} pr
        on tr.reassured::text = pr.party_nk
        and pr.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pr.effective_from and pr.effective_to

    left join {{ref('dim_parties')}} pc
        on tr.client::text = pc.party_nk
        and pc.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pc.effective_from and pc.effective_to

    left join {{ref('dim_parties')}}  pe
        on tr.synd::text = pe.party_nk
        and pe.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pe.effective_from and pe.effective_to

    left join {{ref('dim_parties')}} pt
        on tr.producingteam::text = pt.party_nk
        and pt.source_system = tr.system_source --i know, i know
        and tr.transaction_date between pt.effective_from and pt.effective_to
        
    left join {{ref('dim_parties')}}  ppl 
        on tr.placer::text||'broker' = ppl.party_nk
        and ppl.source_system = tr.system_source
        and tr.transaction_date between ppl.effective_from and ppl.effective_to
        
    left join {{ref('dim_parties')}} ppr 
        on tr.producer::text||'broker' = ppr.party_nk
        and ppr.source_system = tr.system_source
        and tr.transaction_date between ppr.effective_from and ppr.effective_to

    left join {{ref('dim_parties')}}  pre 
        on tr.retailer::text||'broker' = pre.party_nk
        and pre.source_system = tr.system_source
        and tr.transaction_date between pre.effective_from and pre.effective_to

    left join dim_accounting_periods ap  
        on tr.transaction_date::text = ap.date

    left join {{ref('dim_currencies')}} orig_ccy  
        on tr.origccyiso = orig_ccy.isochar_code
        and tr.transaction_date between orig_ccy.effective_from and orig_ccy.effective_to

    left join {{ref('dim_currencies')}} sett_ccy  
        on tr.settccyiso = sett_ccy.isochar_code
        and tr.transaction_date between sett_ccy.effective_from and sett_ccy.effective_to


where 
transaction_orig_amt !=0 or transaction_sett_amt !=0

union

select 

tr.transaction_key,
tr.system_source,
tr.event_source,
tr.nk_transaction,
tr.change_key,
tr.instalmentnum::text as instalmentnum,
tr.is_addition,
tr.contraind as has_contra,
tr.amtpct as transaction_rate,
tr.deductionind as transaction_basis,
tr.transaction_date,
categories.financial_category_key,
t_events.transaction_event_key,
ap.accounting_period_key,
r.risk_key,
cob.class_of_business_key,
sha2('n/a') as claim_key,
sha2('n/a') as claim_peril_key,
sha2('n/a') as loss_event_key,
li.insured_line_key,
tr.inceptiondate::date as inception_date_key,
tr.expirydate::date as expiry_date_key,
(tr.yoa||'-01'||'-01') as uw_year_key,
null as lossdatefrom_key,
null as lossdateto_key,
ifnull(pa.party_key,sha2('Unknown')) as assured_party_key,
ifnull(addr_ass.address_key,sha2('Unknown')) as assured_address_key,
ifnull(pr.party_key,sha2('Unknown')) as reassured_party_key,
ifnull(pc.party_key,sha2('Unknown')) as client_party_key,
ifnull(pe.party_key,sha2('Unknown')) as legal_entity_key,
ifnull(pt.party_key,sha2('Unknown')) as producting_team_key,
ifnull(ppl.party_key,sha2('Unknown')) as placer_key,
ifnull(ppr.party_key,sha2('Unknown')) as producer_key,
ifnull(pre.party_key,sha2('Unknown')) as retailer_key,
orig_ccy.currency_key as orig_currency_key,
sett_ccy.currency_key as sett_currency_key,
transaction_orig_amt as transaction_orig_amt,
transaction_ledger_amt as transaction_sett_amt

from {{ref('stg_fct_booked_b_transactions')}} tr

    left join {{ref('dim_financial_categories')}} categories
        on tr.ftcr_fintranscategoryid::text = categories.nk_financial_category
        
    left join {{ref('stg_booked_b_telookup')}} lkp 
        on categories.sub_category = lkp.lookup_fincategory
        
    left join {{ref('dim_transaction_events')}} t_events
        on 'Inward Booked' = t_events.transaction_event
        and ifnull(lkp.transaction_sub_event,'Other') = t_events.transaction_sub_event 
        
    left join {{ref('dim_risk')}} r
        on tr.policyid = r.risk_nk
        and r.source_system = tr.system_source --stupid
        and tr.transaction_date between r.effective_from and r.effective_to 

    left join dim_class_of_business cob 
        on tr.class4 = cob.tier_1_code
        and tr.class1 = cob.tier_2_code
        and tr.class3 = cob.tier_3_code
        and tr.transaction_date between cob.valid_from and cob.valid_to

    left join {{ref('dim_insured_line')}} li
        on tr.policylineid::text = li.insured_line_nk
        and r.source_system = tr.system_source 
        and tr.transaction_date between li.effective_from and li.effective_to

    left join {{ref('dim_parties')}} pa
        on tr.assured::text = pa.party_nk
        and pa.source_system = tr.system_source 
        and tr.transaction_date between pa.effective_from and pa.effective_to

    left join {{ref('dim_addresses')}} addr_ass
        on tr.assured_addr_id::text = addr_ass.address_nk
        and addr_ass.source_system = tr.system_source 
        and tr.transaction_date between addr_ass.effective_from and addr_ass.effective_to

    left join {{ref('dim_parties')}} pr
        on tr.reassured::text = pr.party_nk
        and pr.source_system = tr.system_source 
        and tr.transaction_date between pr.effective_from and pr.effective_to

    left join {{ref('dim_parties')}} pc
        on tr.client::text = pc.party_nk
        and pc.source_system = tr.system_source 
        and tr.transaction_date between pc.effective_from and pc.effective_to

    left join {{ref('dim_parties')}}  pe
        on tr.synd::text = pe.party_nk
        and pe.source_system = tr.system_source 
        and tr.transaction_date between pe.effective_from and pe.effective_to

    left join {{ref('dim_parties')}} pt
        on tr.producingteam::text = pt.party_nk
        and pt.source_system = tr.system_source 
        and tr.transaction_date between pt.effective_from and pt.effective_to
        
    left join {{ref('dim_parties')}}  ppl 
        on tr.placer::text||'broker' = ppl.party_nk
        and ppl.source_system = tr.system_source
        and tr.transaction_date between ppl.effective_from and ppl.effective_to
        
    left join {{ref('dim_parties')}} ppr 
        on tr.producer::text||'broker' = ppr.party_nk
        and ppr.source_system = tr.system_source
        and tr.transaction_date between ppr.effective_from and ppr.effective_to

    left join {{ref('dim_parties')}}  pre 
        on tr.retailer::text||'broker' = pre.party_nk
        and pre.source_system = tr.system_source
        and tr.transaction_date between pre.effective_from and pre.effective_to

    left join dim_accounting_periods ap  
        on tr.transaction_date::text = ap.date

    left join {{ref('dim_currencies')}} orig_ccy  
        on tr.origccyiso = orig_ccy.isochar_code
        and tr.transaction_date between orig_ccy.effective_from and orig_ccy.effective_to

    left join {{ref('dim_currencies')}} sett_ccy  
        on tr.ledgerccyiso = sett_ccy.isochar_code
        and tr.transaction_date between sett_ccy.effective_from and sett_ccy.effective_to


where 
transaction_orig_amt !=0 or transaction_ledger_amt !=0

union

select 

tr.transaction_key,
tr.system_source,
tr.event_source,
tr.nk_transaction,
tr.change_key,
tr.instalmentnum::text as instalmentnum,
tr.is_addition,
tr.contraind as has_contra,
tr.amtpct as transaction_rate,
tr.deductionind as transaction_basis,
tr.transaction_date,
categories.financial_category_key,
t_events.transaction_event_key,
ap.accounting_period_key,
r.risk_key,
cob.class_of_business_key,
sha2('n/a') as claim_key,
sha2('n/a') as claim_peril_key,
sha2('n/a') as loss_event_key,
li.insured_line_key,
tr.inceptiondate::date as inception_date_key,
tr.expirydate::date as expiry_date_key,
(tr.yoa||'-01'||'-01') as uw_year_key,
null as lossdatefrom_key,
null as lossdateto_key,
ifnull(pa.party_key,sha2('Unknown')) as assured_party_key,
ifnull(addr_ass.address_key,sha2('Unknown')) as assured_address_key,
ifnull(pr.party_key,sha2('Unknown')) as reassured_party_key,
ifnull(pc.party_key,sha2('Unknown')) as client_party_key,
ifnull(pe.party_key,sha2('Unknown')) as legal_entity_key,
ifnull(pt.party_key,sha2('Unknown')) as producting_team_key,
ifnull(ppl.party_key,sha2('Unknown')) as placer_key,
ifnull(ppr.party_key,sha2('Unknown')) as producer_key,
ifnull(pre.party_key,sha2('Unknown')) as retailer_key,
orig_ccy.currency_key as orig_currency_key,
sett_ccy.currency_key as sett_currency_key,
transaction_orig_amt as transaction_orig_amt,
transaction_ledger_amt as transaction_sett_amt

from {{ref('stg_fct_paid_b_transactions')}} tr

    left join {{ref('dim_financial_categories')}} categories
        on tr.ftcr_fintranscategoryid::text = categories.nk_financial_category
        
    --left join {{ref('stg_booked_event_lookup')}} lkp 
    --    on tr.instalmenttype = lkp.lookup_value
        
    left join {{ref('dim_transaction_events')}} t_events
        on 'Inward Cash Allocated' = t_events.transaction_event
        and 'Other' = t_events.transaction_sub_event -- the Swing / Reinstatement are established from InstalmentTypes and you can't (reliably) get that for Bureau policies.
        
    left join {{ref('dim_risk')}} r
        on tr.policyid = r.risk_nk
        and r.source_system = tr.system_source --stupid
        and tr.transaction_date between r.effective_from and r.effective_to 
    
    left join {{ref('dim_insured_line')}} li
        on tr.policylineid::text = li.insured_line_nk
        and r.source_system = tr.system_source 
        and tr.transaction_date between li.effective_from and li.effective_to

    left join {{ref('dim_parties')}} pa
        on tr.assured::text = pa.party_nk
        and pa.source_system = tr.system_source 
        and tr.transaction_date between pa.effective_from and pa.effective_to

    left join {{ref('dim_addresses')}} addr_ass
        on tr.assured_addr_id::text = addr_ass.address_nk
        and addr_ass.source_system = tr.system_source 
        and tr.transaction_date between addr_ass.effective_from and addr_ass.effective_to

    left join {{ref('dim_parties')}} pr
        on tr.reassured::text = pr.party_nk
        and pr.source_system = tr.system_source 
        and tr.transaction_date between pr.effective_from and pr.effective_to

    left join {{ref('dim_parties')}} pc
        on tr.client::text = pc.party_nk
        and pc.source_system = tr.system_source 
        and tr.transaction_date between pc.effective_from and pc.effective_to

    left join {{ref('dim_parties')}}  pe
        on tr.synd::text = pe.party_nk
        and pe.source_system = tr.system_source 
        and tr.transaction_date between pe.effective_from and pe.effective_to

    left join {{ref('dim_parties')}} pt
        on tr.producingteam::text = pt.party_nk
        and pt.source_system = tr.system_source 
        and tr.transaction_date between pt.effective_from and pt.effective_to
        
    left join {{ref('dim_parties')}}  ppl 
        on tr.placer::text||'broker' = ppl.party_nk
        and ppl.source_system = tr.system_source
        and tr.transaction_date between ppl.effective_from and ppl.effective_to
        
    left join {{ref('dim_parties')}} ppr 
        on tr.producer::text||'broker' = ppr.party_nk
        and ppr.source_system = tr.system_source
        and tr.transaction_date between ppr.effective_from and ppr.effective_to

    left join {{ref('dim_parties')}}  pre 
        on tr.retailer::text||'broker' = pre.party_nk
        and pre.source_system = tr.system_source
        and tr.transaction_date between pre.effective_from and pre.effective_to

    left join dim_class_of_business cob 
        on tr.class4 = cob.tier_1_code
        and tr.class1 = cob.tier_2_code
        and tr.class3 = cob.tier_3_code
        and tr.transaction_date between cob.valid_from and cob.valid_to

    left join dim_accounting_periods ap  
        on tr.transaction_date::text = ap.date

    left join {{ref('dim_currencies')}} orig_ccy  
        on tr.origccyiso = orig_ccy.isochar_code
        and tr.transaction_date between orig_ccy.effective_from and orig_ccy.effective_to

    left join {{ref('dim_currencies')}} sett_ccy  
        on tr.ledgerccyiso = sett_ccy.isochar_code
        and tr.transaction_date between sett_ccy.effective_from and sett_ccy.effective_to

where 
transaction_orig_amt !=0 or transaction_ledger_amt !=0
