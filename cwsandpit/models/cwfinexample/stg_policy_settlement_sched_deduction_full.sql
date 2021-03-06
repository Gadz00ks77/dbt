

select 
sssd.settlementschedulesharedeductionid,
ssd.settlementscheduledeductionid,
ssd.deductionind,
ssd.amtpct,
ssd.policysettschedid,
sssd.policysettschedshareid,
ssd.addition,
ssd.fintranscategoryid,
ssd.amtccyiso,
case when sssd.actual_date > sssd.lastupd then sssd.actual_date else sssd.lastupd end as actual_date,
case when sssd.deldate is not null or ssd.deldate is not null then 0 else sssd.totalamt end as totalamt, --see my note on stg_policy_settlement_sched_full
pss.roe,
pss.fintransid, -- this is used as a hook for non-bureau transactions for Booked & Signed to LedgerTrans via FinTransDetail
ssd.lastupd as sched_lastupd,
sssd.lastupd as schedshare_lastupd
    
from {{ ref('stg_policy_settlement_sched_deduction') }} ssd
    join {{ ref('stg_policy_settlement_sched_deduction_share')}} sssd on 
        ssd.settlementscheduledeductionid = sssd.settlementscheduledeductionid
        and ssd.actual_date = sssd.actual_date
    join {{ ref('stg_policy_settlement_sched')}} pss on
        ssd.policysettschedid = pss.policysettschedid
        and ssd.actual_date = pss.actual_date