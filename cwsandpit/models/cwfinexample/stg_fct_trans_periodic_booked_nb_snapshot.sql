
-- this summarises the written transactions into a snapshot day-by-day accumulating view. 

with 

tr_in_range as (

select d.date::date as snapshot_date, * from {{ref('stg_fct_booked_nb_transactions')}} tr

    join architecture_db.cwsandpit.dim_date d on date::date between '2019-12-31' and current_date - 1

where tr.transaction_date < d.date::date --create a fan of transactions over time
)
, last_keys as (

select 
  
  *,
--   last_value(tr.inceptiondate::date) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_inception_date, -- give the last appropriate value in the snapshot range (e.g. the last line record up to that snapshot date)
--   last_value(tr.expirydate::date) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_expiry_date,
--   last_value(tr.yoa::int) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_yoa,
  last_value(tr.assured) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_assured,
  last_value(tr.reassured) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_reassured,
  last_value(tr.client) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_client,
  last_value(tr.synd) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_synd,
  last_value(tr.producingteam) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_producingteam,
  last_value(tr.placer) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_placer,
  last_value(tr.producer) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_producer,
  last_value(tr.retailer) over (partition by tr.snapshot_date,tr.policyid order by tr.transaction_date) as last_retailer
  
  from tr_in_range tr
  
),

cte_cumulative as (
  
select 

snapshot_date,
'eclipse' as source_system,
tr.policyid,
tr.policylineid,
tr.instalmenttype,
tr.is_addition,
tr.financial_sub_category,
-- tr.last_inception_date as inception_date_key,
-- tr.last_expiry_date as expiry_date_key,
-- (tr.last_yoa||'-01'||'-01') as uw_year_key,  
tr.origccyiso,
tr.settccyiso,
tr.last_assured as assured,
tr.last_reassured as reassured,
tr.last_client as client,
tr.last_synd as synd,
tr.last_producingteam as producingteam,
tr.last_placer as placer,
tr.last_producer as producer,
tr.last_retailer as retailer,
sum(transaction_orig_amt) as cumu_transaction_orig_amt,
sum(transaction_sett_amt) as cumu_transaction_sett_amt

from last_keys tr

group by
snapshot_date ,
tr.policyid,
tr.policylineid,
tr.instalmenttype,
tr.is_addition,
tr.financial_sub_category,
tr.origccyiso,
tr.settccyiso,
tr.last_assured,
tr.last_reassured,
tr.last_client,
tr.last_synd,
tr.last_producingteam,
tr.last_placer,
tr.last_producer,
tr.last_retailer
-- tr.last_inception_date,
-- tr.last_expiry_date,
-- (tr.last_yoa||'-01'||'-01') 

)

select
'non-bureau booked' as measure_source, 
cu.source_system,
ap.accounting_period_key,
cu.snapshot_date,
r.risk_key,
li.insured_line_key,
orig_ccy.currency_key as orig_ccy_key,
sett_ccy.currency_key as sett_ccy_key,
last_value(po.inceptiondate::date) over (partition by cu.snapshot_date,r.risk_key order by cu.snapshot_date) as inception_date_key,
last_value(po.expirydate::date) over (partition by cu.snapshot_date,r.risk_key order by cu.snapshot_date) as expiry_date_key,
case when last_value(po.yoa) over (partition by cu.snapshot_date,r.risk_key order by cu.snapshot_date) is null then null else (last_value(po.yoa) over (partition by cu.snapshot_date,r.risk_key order by cu.snapshot_date)||'-01'||'-01')  end uw_year_key,
is_addition,  
ifnull(pa.party_key,sha2('Unknown')) as assured_party_key,
ifnull(pr.party_key,sha2('Unknown')) as reassured_party_key,
ifnull(pc.party_key,sha2('Unknown')) as client_party_key,
ifnull(pe.party_key,sha2('Unknown')) as legal_entity_key,
ifnull(pt.party_key,sha2('Unknown')) as producting_team_key,
ifnull(ppl.party_key,sha2('Unknown')) as placer_key,
ifnull(ppr.party_key,sha2('Unknown')) as producer_key,
ifnull(pre.party_key,sha2('Unknown')) as retailer_key,
t_events.transaction_event_key,
t_events.transaction_event,
t_events.transaction_sub_event,
categories.financial_category_key,
categories.category,
categories.parent_category,
categories.sub_category,
cumu_transaction_orig_amt,
cumu_transaction_sett_amt

from 
cte_cumulative cu
    
    left join {{ref('dim_risk')}} r
        on cu.policyid = r.risk_nk
        and r.source_system = cu.source_system 
        and cu.snapshot_date between r.effective_from and r.effective_to 

    left join {{ref('stg_policies')}} po 
        on cu.policyid = po.policyid
        and cu.snapshot_date = po.actual_date

    left join {{ref('dim_financial_categories')}} categories
        on cu.financial_sub_category::text = categories.nk_financial_category

    left join {{ref('stg_booked_event_lookup')}} lkp 
        on cu.instalmenttype = lkp.lookup_value
        
    left join {{ref('dim_transaction_events')}} t_events
        on lkp.transaction_event = t_events.transaction_event
        and lkp.transaction_sub_event = t_events.transaction_sub_event
           
    left join {{ref('dim_insured_line')}} li
        on cu.policylineid::text = li.insured_line_nk
        and r.source_system = cu.source_system--i know, i know
        and cu.snapshot_date between li.effective_from and li.effective_to

    left join {{ref('dim_parties')}} pa
        on cu.assured::text = pa.party_nk
        and pa.source_system = cu.source_system --i know, i know
        and cu.snapshot_date between pa.effective_from and pa.effective_to

    left join {{ref('dim_parties')}} pr
        on cu.reassured::text = pr.party_nk
        and pr.source_system = cu.source_system --i know, i know
        and cu.snapshot_date between pr.effective_from and pr.effective_to

    left join {{ref('dim_parties')}} pc
        on cu.client::text = pc.party_nk
        and pc.source_system = cu.source_system --i know, i know
        and cu.snapshot_date between pc.effective_from and pc.effective_to

    left join {{ref('dim_parties')}}  pe
        on cu.synd::text = pe.party_nk
        and pe.source_system = cu.source_system --i know, i know
        and cu.snapshot_date between pe.effective_from and pe.effective_to

    left join {{ref('dim_parties')}} pt
        on cu.producingteam::text = pt.party_nk
        and pt.source_system = cu.source_system --i know, i know
        and cu.snapshot_date between pt.effective_from and pt.effective_to
        
    left join {{ref('dim_parties')}} ppl 
        on cu.placer::text||'broker' = ppl.party_nk
        and ppl.source_system = cu.source_system
        and cu.snapshot_date between ppl.effective_from and ppl.effective_to
        
    left join {{ref('dim_parties')}} ppr 
        on cu.producer::text||'broker' = ppr.party_nk
        and ppr.source_system = cu.source_system
        and cu.snapshot_date between ppr.effective_from and ppr.effective_to

    left join {{ref('dim_parties')}} pre 
        on cu.retailer::text||'broker' = pre.party_nk
        and pre.source_system = cu.source_system
        and cu.snapshot_date between pre.effective_from and pre.effective_to

    left join dim_accounting_periods ap  
        on cu.snapshot_date::text = ap.date

    left join currency_dim orig_ccy  
        on ifnull(cu.origccyiso,cu.settccyiso) = orig_ccy.isochar_code
        and cu.snapshot_date between orig_ccy.effective_from and orig_ccy.effective_to

    left join currency_dim sett_ccy  
        on cu.settccyiso = sett_ccy.isochar_code
        and cu.snapshot_date between sett_ccy.effective_from and sett_ccy.effective_to

--where parent_category = 'Premium'
