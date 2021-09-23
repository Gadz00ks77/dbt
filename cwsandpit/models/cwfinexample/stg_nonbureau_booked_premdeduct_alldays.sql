
select 
    lt.actual_date,
    lt.ledgertransid::text||lt.actual_date::text as dupe_checker,
    psu.policyid,
    psu.policylineid,
    psu.policysettschedid,
    psu.policysettschedshareid,
    psu.settlementschedulesharedeductionid,
    psu.settlementscheduledeductionid,
    psu.is_addition,
    psu.instalmentnum,
    psu.instalmenttype,
    psu.amtpct,
    psu.deductionind,
    psu.linestatus,
    psu.producingteam,
    psu.synd,
    psu.roe,
    psu.origccyiso,
    psu.settccyiso,
    psu.inceptiondate,
    psu.expirydate,
    psu.yoa,
    psu.assured,
    psu.assured_addr_id,
    psu.reassured,
    psu.client,
    psu.producer,
    psu.retailer,
    psu.placer,
    psu.class1,
    psu.class2,
    psu.class3,
    psu.class4,
    lt.ledgertransid,
    lt.contraind,
    lt.upddate,
    lt.origamt as orig_amt,
    lt.ledgeramt as sett_amt,
    lt.ledgeraccountid,
    ftd.fintransid,
    psu.financial_category,
    ftcr.fintranscategoryid

    from 
        {{ref('stg_ledgertrans')}} LT
        
        join {{ref('stg_fintransdetail')}} FTD
            on lt.fintransdetailid = ftd.fintransdetailid
            and lt.actual_date = ftd.actual_date

        join {{ref('stg_fintrans')}} FT
            on ft.fintransid = ftd.fintransid
            and ft.actual_date = ftd.actual_date

        join {{ref('stg_policies')}} pl
            on ft.policyid = pl.policyid            
            and ft.actual_date = pl.actual_date

        jOIN {{ref('stg_fintranscategoryrole')}} FTCR 
            ON FTD.FinTransCategoryRoleId = FTCR.FinTransCategoryRoleId 
            and ftd.actual_date = ftcr.actual_date
            
        join {{ref('stg_policy_settlement_union')}} psu 
            on ft.fintransid = psu.fintransid
            and ft.policylineid = psu.policylineid
            and ft.actual_date = psu.transaction_date
            and case when ftcr.fintranscategoryid in (59) then -1 else ftcr.fintranscategoryid end = psu.financial_sub_category
                    --- I hate the above join. 59 is the premium category and it appears to be used exclusively but if they change it...
            
         where LT.ledgerTransType = 'B' -- is a broking ledger transaction
            and pl.bureausettledind = 'N' -- is non-bureau
   
   