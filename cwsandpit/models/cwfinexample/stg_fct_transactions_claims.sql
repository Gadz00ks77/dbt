-- transactions (from essentially snapshot data in part) for claims

with market_paid_indem as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Paid - Market' as Transaction_SubEvent,
       'Indemnity' as Category_Description,
       'Claim' as Parent_Category_Group,
       m.marketpaidthistimeindemorigccy as transaction_original,
       m.marketpaidthistimeindemsettccy as transaction_settlement
    from  {{ref('stg_movement')}} m

        where (abs(m.marketpaidthistimeindemorigccy) > 0 or abs(m.marketpaidthistimeindemsettccy) > 0)
        and m.is_valid = 'TRUE' 

        -- the movement table gets updated. Sometimes affecting the values. Need to account for this in a future version, because
        -- if we don't then the history in Stn-X will change once they change the paid. Not many records do this though (with monetary adjustments, that is).
        -- Interesting presence of a contra value on the movement table - needs to be investigated.

), market_paid_fee as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Paid - Market' as Transaction_SubEvent,
       'Fee' as Category_Description,
       'Claim' as Parent_Category_Group,
       m.marketpaidthistimefeeorigccy as transaction_original,
       m.marketpaidthistimefeesettccy as transaction_settlement
    from  {{ref('stg_movement')}} m

        where (abs(m.marketpaidthistimefeeorigccy) > 0 or abs(m.marketpaidthistimefeesettccy) > 0)
        and m.is_valid = 'TRUE' 

), share_paid_indem as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Paid - Share' as Transaction_SubEvent,
       'Indemnity' as Category_Description,
       'Claim' as Parent_Category_Group,
       m.marketpaidthistimeindemorigccy*(signedline/100) as transaction_original,
       m.marketpaidthistimeindemsettccy*(signedline/100) as transaction_settlement
    from  {{ref('stg_movement')}} m

        join {{ref('stg_claimline')}} cli 
            on m.movementdate::date  = cli.actual_date
            and m.claimid = cli.claimid 

        where (abs(m.marketpaidthistimeindemorigccy) > 0 or abs(m.marketpaidthistimeindemsettccy) > 0)
         and m.is_valid = 'TRUE'        

), share_paid_fee as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Paid - Share' as Transaction_SubEvent,
       'Fee' as Category_Description,
       'Claim' as Parent_Category_Group,
       m.marketpaidthistimefeeorigccy*(signedline/100) as transaction_original,
       m.marketpaidthistimefeesettccy*(signedline/100) as transaction_settlement
    from  {{ref('stg_movement')}} m

        join {{ref('stg_claimline')}} cli 
            on m.movementdate::date  = cli.actual_date
            and m.claimid = cli.claimid 

        where (abs(m.marketpaidthistimefeeorigccy) > 0 or abs(m.marketpaidthistimefeesettccy) > 0)
        and m.is_valid = 'TRUE' 

), market_os_indem as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Outstanding Movement - Market' as Transaction_SubEvent,
       'Indemnity' as Category_Description,
       'Claim' as Parent_Category_Group,
       ifnull(lag(marketosindemorigccy) ignore nulls over(partition by claimid, origccyiso order by movementid),0) as transaction_lag_original,
       marketosindemorigccy - transaction_lag_original as transaction_original,
       ifnull(lag(marketosindemsettccy) ignore nulls over(partition by claimid, settccyiso order by movementid),0) as transaction_lag_settlement,
       marketosindemsettccy - transaction_lag_settlement as transaction_settlement
    
    from  {{ref('stg_movement')}} m

        where m.is_valid = 'TRUE' 

), market_os_fee as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Outstanding Movement - Market' as Transaction_SubEvent,
       'Fee' as Category_Description,
       'Claim' as Parent_Category_Group,
       ifnull(lag(marketosfeeorigccy) ignore nulls over(partition by claimid, origccyiso order by movementid),0) as transaction_lag_original,
       marketosfeeorigccy - transaction_lag_original as transaction_original,
       ifnull(lag(marketosfeesettccy) ignore nulls over(partition by claimid, settccyiso order by movementid),0) as transaction_lag_settlement,
       marketosfeesettccy - transaction_lag_settlement as transaction_settlement
    
    from  {{ref('stg_movement')}} m

        where m.is_valid = 'TRUE'


), share_os_indem as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Outstanding Movement - Share' as Transaction_SubEvent,
       'Indemnity' as Category_Description,
       'Claim' as Parent_Category_Group,
       ifnull(lag(marketosindemorigccy) ignore nulls over(partition by m.claimid, origccyiso order by movementid),0) as transaction_lag_original,
       (marketosindemorigccy - transaction_lag_original)*(signedline/100) as transaction_original,
       ifnull(lag(marketosindemsettccy) ignore nulls over(partition by m.claimid, settccyiso order by movementid),0) as transaction_lag_settlement,
       (marketosindemsettccy - transaction_lag_settlement)*(signedline/100) as transaction_settlement
    
    from  {{ref('stg_movement')}} m

        join {{ref('stg_claimline')}} cli 
            on m.movementdate::date  = cli.actual_date
            and m.claimid = cli.claimid 

        where m.is_valid = 'TRUE'


), share_os_fee as (

    select m.claimid,
       m.movementid,
       m.movementdate,
       m.movementdate::date as movementdateonly,
       m.origccyiso,
       m.settccyiso,
       'Inward Claims' as Transaction_Event,
       'Outstanding Movement - Share' as Transaction_SubEvent,
       'Fee' as Category_Description,
       'Claim' as Parent_Category_Group,
       ifnull(lag(marketosfeeorigccy) ignore nulls over(partition by m.claimid, origccyiso order by movementid),0) as transaction_lag_original,
       (marketosfeeorigccy - transaction_lag_original)*(signedline/100) as transaction_original,
       ifnull(lag(marketosfeesettccy) ignore nulls over(partition by m.claimid, settccyiso order by movementid),0) as transaction_lag_settlement,
       (marketosfeesettccy - transaction_lag_settlement)*(signedline/100) as transaction_settlement
    
    from  {{ref('stg_movement')}} m

        join {{ref('stg_claimline')}} cli 
            on m.movementdate::date  = cli.actual_date
            and m.claimid = cli.claimid 

        where m.is_valid = 'TRUE'

),

cte_union as (

    select * from market_paid_indem
    union all select * from market_paid_fee
    union all select * from share_paid_indem
    union all select * from share_paid_fee
    union all select 
        claimid,
        movementid,
        movementdate,
        movementdateonly,
        origccyiso,
        settccyiso,
        Transaction_Event,
        Transaction_SubEvent,
        Category_Description,
        Parent_Category_Group,
        transaction_original,
        transaction_settlement
            from market_os_indem
    union all select 
        claimid,
        movementid,
        movementdate,
        movementdateonly,
        origccyiso,
        settccyiso,
        Transaction_Event,
        Transaction_SubEvent,
        Category_Description,
        Parent_Category_Group,
        transaction_original,
        transaction_settlement
            from market_os_fee
    union all select 
        claimid,
        movementid,
        movementdate,
        movementdateonly,
        origccyiso,
        settccyiso,
        Transaction_Event,
        Transaction_SubEvent,
        Category_Description,
        Parent_Category_Group,
        transaction_original,
        transaction_settlement
            from share_os_indem
    union all select 
        claimid,
        movementid,
        movementdate,
        movementdateonly,
        origccyiso,
        settccyiso,
        Transaction_Event,
        Transaction_SubEvent,
        Category_Description,
        Parent_Category_Group,
        transaction_original,
        transaction_settlement
            from share_os_fee
)

    select 

        sha2(u.claimid::text||u.movementid::text||u.movementdate::text||u.transaction_event||u.transaction_subevent||u.Category_Description) as transaction_key,
        'eclipse' as system_source,
        'Inward Claims' as event_source,
        object_construct(
            '0_event_source','Inward Claims',
            '1_movementid',u.movementid,
            '2_movementdate',u.movementdate,
            '3_event',u.transaction_event,
            '4_subevent',u.transaction_subevent,
            '5_financial_category',u.Category_Description) as nk_transaction,
        (u.claimid::text||u.movementdate::text||u.transaction_event||u.transaction_subevent||u.Category_Description) as change_key,
        u.claimid,
        cl.claimperil,
        ifnull(pl.policyid,lead(pl.policyid) ignore nulls over (partition by u.claimid order by movementdateonly)) as policyid, --due to misalignments on the ingestion points of files (or the updates of associated rows) take the first applicable value from the associated record. This is valid. (No Policy = No Claim)
        ifnull(pl.class1,lead(pl.class1) ignore nulls over (partition by u.claimid order by movementdateonly)) as class1,
        ifnull(pl.class2,lead(pl.class2) ignore nulls over (partition by u.claimid order by movementdateonly)) as class2,
        ifnull(pl.class3,lead(pl.class3) ignore nulls over (partition by u.claimid order by movementdateonly)) as class3,
        ifnull(pl.class4,lead(pl.class4) ignore nulls over (partition by u.claimid order by movementdateonly)) as class4,
        ifnull(polline.synd,lead(polline.synd) ignore nulls over (partition by u.claimid order by movementdateonly)) as synd,
        ifnull(polline.producingteam,lead(polline.producingteam) ignore nulls over (partition by u.claimid order by movementdateonly)) as producingteam,
        ifnull(polline.policylineid, lead(polline.policylineid) ignore nulls over (partition by u.claimid order by movementdateonly)) as policylineid,
        ifnull(pl.inceptiondate, lead(pl.inceptiondate) ignore nulls over (partition by u.claimid order by movementdateonly)) as inceptiondate,
        ifnull(pl.expirydate,lead(pl.expirydate) ignore nulls over (partition by u.claimid order by movementdateonly)) as expirydate,
        ifnull(pl.yoa, lead(pl.yoa) ignore nulls over (partition by u.claimid order by movementdateonly)) as yoa,
        ifnull(scl.lossdatefrom, lead(scl.lossdatefrom) ignore nulls over (partition by u.claimid order by movementdateonly))::date as lossdatefrom,
        ifnull(scl.lossdateto, lead(scl.lossdateto) ignore nulls over (partition by u.claimid order by movementdateonly))::date as lossdateto,
        ifnull(org_piv.assured, lead(org_piv.assured) ignore nulls over (partition by u.claimid order by movementdateonly)) as assured,
        ifnull(addr.addrid, lead(addr.addrid) ignore nulls over (partition by u.claimid order by movementdateonly)) as assured_address,
        ifnull(org_piv.reassured, lead(org_piv.reassured) ignore nulls over (partition by u.claimid order by movementdateonly)) as reassured,
        ifnull(org_piv.client, lead(org_piv.client) ignore nulls over (partition by u.claimid order by movementdateonly)) as client,
        ifnull(bro_piv.producer, lead(bro_piv.producer) ignore nulls over (partition by u.claimid order by movementdateonly)) as producer,
        ifnull(bro_piv.placer, lead(bro_piv.placer) ignore nulls over (partition by u.claimid order by movementdateonly)) as placer,
        ifnull(bro_piv.retailer, lead(bro_piv.retailer) ignore nulls over (partition by u.claimid order by movementdateonly)) as retailer,
        ce.claimeventid,
        u.movementid,
        u.movementdate,
        u.movementdateonly,
        u.origccyiso,
        u.settccyiso,
        u.Transaction_Event,
        u.Transaction_SubEvent,
        u.Category_Description,
        u.Parent_Category_Group,
        u.transaction_original as transaction_orig_gross_amt,
        u.transaction_settlement as transaction_sett_gross_amt

        from cte_union u

            left join {{ref('stg_claim')}} cl
                on u.claimid = cl.claimid
                and cl.actual_date = u.movementdateonly

            left join {{ref('stg_claim_sequel_claim')}} scl 
                on cl.claimid = scl.oldclaimref
                and cl.actual_date = scl.actual_date

            left join {{ref('stg_claimline')}} cli
                on cli.claimid = cl.claimid
                and cli.actual_date = cl.actual_date


            left join {{ref('stg_claimevent')}} ce
                on ce.claimeventid = cli.claimeventid
                and ce.actual_date = cli.actual_date

            left join {{ref('stg_claimpolicy')}} clpl 
                on clpl.claimid = cl.claimid 
                and clpl.actual_date = cl.actual_date

            left join {{ref('stg_policies')}} pl 
                on pl.policyid = clpl.policyid 
                and clpl.actual_date = pl.actual_date

            left join {{ref('stg_policylines')}} polline 
                on pl.policyid = polline.policyid 
                and pl.actual_date = polline.actual_date

            left join {{ ref('stg_policy_organisations_pivoted')}} org_piv 
                on pl.policyid = org_piv.policyid 
                and pl.actual_date = org_piv.actual_date

            left join {{ref('stg_addr')}} addr 
                on org_piv.assured = addr.orgid 
                and org_piv.actual_date = addr.actual_date
                and addr.addrtype = 'Head Office' --some assured have multiple office types; for simplicity chosen head office only otherwise fanning. (We've still got one line of dupe in here but I'm not letting that bother for now - this is quick and dirty really)

            left join {{ ref('stg_policy_brokers_pivoted') }} bro_piv 
                on pl.policyid = bro_piv.policyid 
                and pl.actual_date = bro_piv.actual_date

    where u.transaction_original !=0
    or 
    u.transaction_settlement !=0