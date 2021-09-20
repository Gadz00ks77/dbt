
with cte_nk_deriv as (
select 

'eclipse'                                               as system_source,
'written'                                               as event_source,
object_construct(
   '0_event_source','written',
   '1_policysettschedshareid',u.policysettschedshareid,
   '2_settlementschedulesharedeductionid',u.settlementschedulesharedeductionid,
   '3_Financial_Sub_Category',u.Financial_Sub_Category
            ) as nk_transaction, --created an object as I'd like to see "where" the rows have come from in code. Non tech users won't need to / be able to see this.
ifnull(u.origgross::text,'0')||'|'||
ifnull(u.settgross::text,'0')||'|'||
ifnull(u.origccyiso::text,'0')||'|'||                          
ifnull(u.settccyiso::text,'0')              as change_key, -- change keys on facts are not strictly necessary as I don't update this.
u.policyid,
u.policylineid,
u.policysettschedid,
u.policysettschedshareid,
u.settlementschedulesharedeductionid,
u.settlementscheduledeductionid,
u.is_addition,
u.amtpct,
u.deductionind,
u.transaction_date,
u.Financial_Category,
u.Financial_Sub_Category,
u.instalmentnum,
u.instalmenttype,
u.linestatus,
u.origgross,
u.settgross,
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
--written transactions staging
--the data on policy premium schedule / deduction schedule is stored as a snapshot natively in the system, but we want to understand the how the snapshots are updated, and present that as a transaction movement value.
--the Ledger Trans table do NOT do this for money (although they do get updated for contras) - those tables are transactional primarily.

from {{ref('stg_policy_settlement_union')}} u

where 
   u.linestatus in ('Written','Signed','Canc')
  )
  
  ,
  get_change as (
  select *,
  conditional_change_event(change_key) over (partition by nk_transaction order by transaction_date) as change_event
    from cte_nk_deriv
  order by transaction_date
    )
  ,lag_change as (
 
    select * 
    ,ifnull(lag(change_event) over (partition by nk_transaction order by transaction_date),-1) as lag_change_val
    
     from 
    get_change
    
   )
   ,
   lag_vals as (
   
   select 
   
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
   transaction_date,
   inceptiondate,
   expirydate,
   yoa,
   amtpct,
   deductionind,
   financial_category,
   financial_sub_category,
   instalmentnum,
   instalmenttype,
   synd,
   producingteam,
   roe,
   ifnull(lag(origgross) over (partition by nk_transaction order by lag_change_val),0) as lagged_origgross,
   origgross,
   origccyiso,
   ifnull(lag(settgross) over (partition by nk_transaction order by lag_change_val),0) as lagged_settgross,
   settgross,
   settccyiso,
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
   class4

   
   from lag_change
   where change_event != lag_change_val
   
     ),
     
   calc_trans_val as (
     
     
   select 
     
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
   transaction_date,
   inceptiondate,
   expirydate,
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
   yoa,
   synd,
   producingteam,
   financial_category,
   financial_sub_category,
   instalmentnum,
   instalmenttype,
   roe,
   origccyiso,
   settccyiso
   ,origgross - lagged_origgross as transaction_orig_gross_amt
   ,settgross - lagged_settgross as transaction_sett_gross_amt
   
   from lag_vals

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
   ifnull(transaction_orig_gross_amt,transaction_sett_gross_amt*roe) as transaction_orig_gross_amt,
   transaction_sett_gross_amt
  
  from calc_trans_val
   order by nk_transaction,transaction_date