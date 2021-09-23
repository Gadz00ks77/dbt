
select 
    psss.policysettschedshareid,
    pss.policysettschedid,
    pss.policyid,
    pss.actual_date,
    pss.instalmentnum,
    pss.instalmenttype,
    pss.roe,
    pss.fintransid,
    psss.policylineid,
    case when pss.deldate is not null then 0 else psss.origgross end as origgross, --so it seems like WNS occasionally delete rows from pss (and possibly ssd / sssd but not psss) - this accounts for that by setting the value to zero (and the lag will therefore gen a change)
    case when pss.deldate is not null then 0 else psss.settgross end as settgross,
    psss.origccyiso,
    psss.settccyiso,
    pss.lastupd as sched_lastupd,
    psss.lastupd as schedshare_lastupd

from {{ ref('stg_policy_settlement_sched') }} pss 
    join {{ ref('stg_policy_settlement_sched_share')}} psss on 
        pss.policysettschedid = psss.policysettschedid
        and pss.actual_date = psss.actual_date