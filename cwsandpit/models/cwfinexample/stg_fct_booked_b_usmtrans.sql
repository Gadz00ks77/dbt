select 

    lt.ledgertransid,
    usm.usmsigningtransid,
    usm.statusid,
    uvl.reportable,
    usm.origtransid,
    usm.actual_date,
    lt.createddate,
    pl.policylineid,
    lt.ledgertranstype,
    lt.ledgertransstatus,
    ft.fintransid,
    ftcr.fintranscategoryid as ftcr_fintranscategoryid,
    ftc.category,
    usm.EarlyRIInd,
    lt.contraind,
    usm.hascontra,
    usm.iscontra,
    lt.origccyiso,
    case when reportable = 'Y' then lt.origamt else 0 end as origamt,
    case when reportable = 'Y' then lt.ledgeramt else 0 end as ledgeramt,
    lt.ledgerccyiso,
    pl.signednum,
    --usm.sharenetamtorigccy,
    --usm.sharenetamtsettccy,
    usm.signingnum,
    usm.qualcategory,
    usm.inwardsoutwardsind,
    usm.fintranscategoryid as usm_fintranscategoryid,
    usm.policysettschedshareid,
    pl.policyid,
    pl.linestatus,
    pl.producingteam,
    pl.synd,
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

    from {{ref('stg_usmsigningtrans')}} usm
    
        join {{ref('stg_fintrans')}} ft
            on usm.fintransid = ft.fintransid     
            and usm.actual_date = ft.actual_date   

        join {{ref('stg_usmstatus_validlookup')}} uvl 
            on usm.statusid = uvl.usmsigningstatusid
        
        join {{ref('stg_fintransdetail')}} ftd
            on ft.fintransid = ftd.fintransid
            and ft.actual_date = ftd.actual_date

        join {{ref('stg_ledgertrans')}} lt
            on lt.fintransdetailid = ftd.fintransdetailid
            and lt.ledgertranstype = 'B'
            and lt.actual_date = ftd.actual_date
    
        join {{ref('stg_fintranscategoryrole')}} ftcr
            on ftd.fintranscategoryroleid = ftcr.fintranscategoryroleid
            and ftd.actual_date = ftcr.actual_date
            
        join {{ref('qualcodemapping')}} qc
            on ftcr.fintranscategoryid = qc.fintranscategoryid         
            
        left join {{ref('fintransbuscatqualcodereallydumbmapping')}} dm 
            on dm.qualcategory::text = qc.qualcategorycode::text and dm.entrytype = usm.entrytype

        left join {{ref('stg_fintranscategory')}} ftc
            on ftc.fintranscategoryid = ifnull(qc.fintranscategoryid,usm.fintranscategoryid)
            and ftc.actual_date = usm.actual_date

        join {{ref('stg_policylines')}} pl   
            on ft.policylineid = pl.policylineid
            and ft.actual_date = pl.actual_date
            
        join {{ref('stg_policies')}} p
            on p.policyid = pl.policyid
            and p.actual_date = pl.actual_date      

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

-- All of the where clauses below make the assumption that the data in those fields never changes. If it does then we must be
-- careful that the data won't shift historically as a result.

        where 
            -- p.bureausettledind = 'Y' -- removed this because sometimes bureau policies are marked as non-bureau. Here ,here for fucking idiocy.
            --and pl.policylineid = 2346 --remove me later
            --and
            ftc.groupname != 'C' -- remove claims
            and ifnull(usm.EarlyRIInd,'N') = 'N'
            and usm.Source in ('USM','ILUCSM','DSIGN') 
            and usm.CategoryCode in ('0', '1', '2', '3', '4', '5')
            and (qc.QualCategoryCode = usm.QualCategory or dm.OrigQualCategory = usm.QualCategory) -- resolves the fanning on the join. See note below on the join

-- The logic for this query is taken from the Eclipse documentation for loading into EAA.
-- It matches the view EnquiryWrittenvsBookedvsPaid (for Booked) on the policies I've reviewed.
-- Two tables from Eclipse are brought in here as seeds (the lookup tables for qual code / fin trans to business category)
-- Looks like Eclipse have cludged all this to be honest. Bit of a mess.