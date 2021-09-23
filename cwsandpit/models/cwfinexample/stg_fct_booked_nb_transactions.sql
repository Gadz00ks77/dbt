
with cte_nk_deriv as (
select
'eclipse'                                               as system_source,
'non-bureau booked'                                     as event_source,
object_construct(
   '0_event_source','non-bureau booked',
   '1_ledgertransid',u.ledgertransid
            ) as nk_transaction, --created an object as I'd like to see "where" the rows have come from in code. Non tech users won't need to / be able to see this.
ifnull(u.orig_amt::text,'0')||'|'||
ifnull(u.sett_amt::text,'0')||'|'||
ifnull(u.contraind::text,'0')
as change_key, -- change keys on facts are not strictly necessary as I don't update this.
u.policyid,
u.policylineid,
u.policysettschedid,
u.policysettschedshareid,
u.settlementschedulesharedeductionid,
u.settlementscheduledeductionid,
u.is_addition,
u.amtpct,
u.deductionind,
u.contraind,
u.actual_date as transaction_date,
u.Financial_Category,
u.fintranscategoryid as Financial_Sub_Category,
u.instalmentnum,
u.instalmenttype,
u.linestatus,
u.orig_amt,
u.sett_amt,
u.origccyiso,
u.settccyiso,
u.roe,
u.synd,
u.producingteam,
u.inceptiondate,
u.expirydate,
u.yoa,
u.assured,
u.assured_addr_id,
u.reassured,
u.client,
u.producer,
u.placer,
u.retailer,
u.class1,
u.class2,
u.class3,
u.class4
  
  from {{ref('stg_nonbureau_booked_premdeduct_alldays')}} u

where linestatus in ('Written','Signed','Canc')

),
  get_change as (

   select *,
    conditional_change_event(change_key) 
    over (partition by nk_transaction order by transaction_date) as change_event
    
    from cte_nk_deriv

  ),
  
  lag_change as (
 
    select * 
    ,ifnull(lag(change_event) over (partition by nk_transaction order by transaction_date),-1) as lag_change_val
    
     from 
    get_change
        ),

  lag_vals as (

select
  lc.system_source,
  lc.event_source,
  lc.nk_transaction, 
  lc.change_key, 
  lc.policyid,
  lc.policylineid,
  lc.policysettschedid,
  lc.policysettschedshareid,
  lc.settlementschedulesharedeductionid,
  lc.settlementscheduledeductionid,
  lc.is_addition,
  lc.amtpct,
  lc.deductionind,
  lc.contraind,
  ifnull(lag(contraind) over (partition by nk_transaction order by lag_change_val),0) as lagged_contraind,
  lc.transaction_date,
  lc.Financial_Category,
  lc.Financial_Sub_Category,
  lc.instalmentnum,
  lc.instalmenttype,
  lc.linestatus,
  lc.orig_amt,
  ifnull(lag(orig_amt) over (partition by nk_transaction order by lag_change_val),0) as lagged_orig_amt,
  lc.sett_amt,
  ifnull(lag(sett_amt) over (partition by nk_transaction order by lag_change_val),0) as lagged_sett_amt,
  lc.origccyiso,
  lc.settccyiso,
  lc.roe,
  lc.synd,
  lc.producingteam,
  lc.inceptiondate,
  lc.expirydate,
  lc.yoa,
  lc.assured,
  lc.assured_addr_id,
  lc.reassured,
  lc.client,
  lc.producer,
  lc.placer,
  lc.retailer,
  lc.class1,
  lc.class2,
  lc.class3,
  lc.class4    
from lag_change lc
    where 
        lc.change_event != lag_change_val
  
    ),
    
    calc_trans_vals as 
    
    (
    
select
  lc.system_source,
  lc.event_source,
  lc.nk_transaction, 
  lc.change_key, 
  lc.policyid,
  lc.policylineid,
  lc.policysettschedid,
  lc.policysettschedshareid,
  lc.settlementschedulesharedeductionid,
  lc.settlementscheduledeductionid,
  lc.is_addition,
  lc.amtpct,
  lc.deductionind,
  lc.contraind,
  lc.lagged_contraind,
  lc.transaction_date,
  lc.Financial_Category,
  lc.Financial_Sub_Category,
  lc.instalmentnum,
  lc.instalmenttype,
  lc.linestatus,
  lc.orig_amt,
  lc.lagged_orig_amt,
  lc.sett_amt,
  lc.lagged_sett_amt,
  case when contraind = 1 then orig_amt - lagged_orig_amt * -1 else orig_amt - lagged_orig_amt end as transaction_orig_amt, -- so, if the amount gets contra'd we want to report it when it wasn't contra'd and then when it was - reversing the original out.
  case when contraind = 1 then sett_amt - lagged_sett_amt * -1 else sett_amt - lagged_sett_amt end as transaction_sett_amt, -- as above, no examples in non-bureau booked but worth checking when we get to the other stuff.
  lc.origccyiso,
  lc.settccyiso,
  lc.roe,
  lc.synd,
  lc.producingteam,
  lc.inceptiondate,
  lc.expirydate,
  lc.yoa,
  lc.assured,
  lc.assured_addr_id,
  lc.reassured,
  lc.client,
  lc.producer,
  lc.placer,
  lc.retailer,
  lc.class1,
  lc.class2,
  lc.class3,
  lc.class4    
from lag_vals lc

    
    )
    
    
   select 
   
   sha2(nk_transaction::text||transaction_date::text||change_key::text) as transaction_key,
   system_source,
   event_source,
   nk_transaction,
   change_key,
   policyid,
   policylineid,
   policysettschedid,
   policysettschedshareid,
   settlementschedulesharedeductionid,
   settlementscheduledeductionid,
   is_addition,
   amtpct,
   deductionind,
   contraind,
   transaction_date,
   inceptiondate,
   expirydate,
   yoa,
   financial_category,
   financial_sub_category,
   instalmentnum,
   instalmenttype,
   synd,
   producingteam,
   assured,
   assured_addr_id,
   reassured,
   client,
   producer,
   placer,
   retailer,
   class1,
   class2,
   class3,
   class4,
   roe,
   origccyiso,
   settccyiso,
   transaction_orig_amt,
   transaction_sett_amt
  
  from calc_trans_vals
   order by nk_transaction,transaction_date 


