
select 
p.policyid,
pl.policylineid,
psf.policysettschedid,
psf.policysettschedshareid,
-1 as settlementschedulesharedeductionid,
-1 as settlementscheduledeductionid,
'N' as is_addition,
psf.actual_date as transaction_date,
'Premium' as Financial_Category,
-1 as Financial_Sub_Category,
psf.instalmentnum,
psf.instalmenttype,
null as amtpct,
'A' as deductionind,
pl.linestatus,
pl.producingteam,
pl.synd,
psf.roe,
psf.fintransid, --- added 21/09
psf.origgross,
psf.settgross,
psf.origccyiso,
psf.settccyiso,
psf.sched_lastupd,
psf.schedshare_lastupd,
p.inceptiondate,
p.expirydate,
p.yoa,
p.bureausettledind,
org_piv.assured,
addr.addrid as assured_addr_id,
org_piv.reassured,
org_piv.client,
bro_piv.producer,
bro_piv.placer,
bro_piv.retailer,
p.class1,
p.class2,
p.class3,
p.class4

from {{ ref('stg_policy_settlement_sched_full') }} psf

        join {{ ref('stg_policies') }} p 
            on psf.policyid = p.policyid
            and psf.actual_date = p.actual_date
        
        join {{ ref('stg_policylines') }} pl 
            on psf.policylineid = pl.policylineid
            and psf.actual_date = pl.actual_date

        left join {{ ref('stg_policy_organisations_pivoted')}} org_piv 
            on p.policyid = org_piv.policyid 
            and p.actual_date = org_piv.actual_date

        left join {{ref('stg_addr')}} addr 
            on org_piv.assured = addr.orgid 
            and org_piv.actual_date = addr.actual_date
            and addr.addrtype = 'Head Office' --some assured have multiple office types; for simplicity chosen head office only otherwise fanning.

        left join {{ ref('stg_policy_brokers_pivoted') }} bro_piv 
            on p.policyid = bro_piv.policyid 
            and p.actual_date = bro_piv.actual_date

union 

select 
p.policyid,
pl.policylineid,
pdf.policysettschedid,
psss.policysettschedshareid,
pdf.settlementschedulesharedeductionid as settlementschedulesharedeductionid,
pdf.settlementscheduledeductionid as settlementscheduledeductionid,
pdf.addition as is_addition,
pdf.actual_date as transaction_date,
'Deduction' as Financial_Category,
pdf.fintranscategoryid as Financial_Sub_Category,
pss.instalmentnum,
pss.instalmenttype,
pdf.amtpct,
pdf.deductionind,
pl.linestatus,
pl.producingteam,
pl.synd,
pdf.roe,
pdf.fintransid, --added 21/09
null as origgross,
pdf.totalamt as settgross,
pss.default_origccyiso as origccyiso,
pdf.amtccyiso as settccyiso,
pdf.sched_lastupd,
pdf.schedshare_lastupd,
p.inceptiondate,
p.expirydate,
p.yoa,
p.bureausettledind,
org_piv.assured,
addr.addrid as assured_addr_id,
org_piv.reassured,
org_piv.client,
bro_piv.producer,
bro_piv.placer,
bro_piv.retailer,
p.class1,
p.class2,
p.class3,
p.class4


from {{ ref('stg_policy_settlement_sched_deduction_full') }} pdf

        join {{ ref('stg_policy_settlement_sched') }} pss
            on pdf.policysettschedid = pss.policysettschedid
            and pdf.actual_date = pss.actual_date
        
        join {{ ref('stg_policy_settlement_sched_share') }} psss 
            on pss.policysettschedid = psss.policysettschedid
            and pdf.policysettschedshareid = psss.policysettschedshareid
            and pss.actual_date = psss.actual_date
            
        join {{ ref('stg_policies') }} p 
            on pss.policyid = p.policyid
            and pss.actual_date = p.actual_date
        
        join {{ ref('stg_policylines') }} pl 
            on psss.policylineid = pl.policylineid
            and psss.actual_date = pl.actual_date

        join {{ ref('stg_policy_organisations_pivoted')}} org_piv 
            on p.policyid = org_piv.policyid 
            and p.actual_date = org_piv.actual_date

        left join {{ref('stg_addr')}} addr 
            on org_piv.assured = addr.orgid 
            and org_piv.actual_date = addr.actual_date
            and addr.addrtype = 'Head Office' --some assured have multiple office types; for simplicity chosen head office only otherwise fanning.

        left join {{ ref('stg_policy_brokers_pivoted') }} bro_piv 
            on p.policyid = bro_piv.policyid 
            and p.actual_date = bro_piv.actual_date            