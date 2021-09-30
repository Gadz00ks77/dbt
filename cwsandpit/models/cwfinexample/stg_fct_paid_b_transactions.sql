


with cte_nk_deriv as (

select 

'eclipse' as system_source,
'paid bureau' as event_source,
object_construct(
'0_event_source', 'paid bureau',
'1_ledgertransid', b.ledgertransid,
'2_usmsigningtransid', b.usmsigningtransid
) as nk_transaction,
ifnull(b.origamt::text,'0')||'|'||
ifnull(b.ledgeramt::text,'0')||'|'||
ifnull(b.origccyiso::text,'0')||'|'||                          
ifnull(b.ledgerccyiso::text,'0')||'|'||              
ifnull(b.ledgertransstatus::text,'0')
as change_key,
b.policyid,
b.policylineid,
case when a.category_lookup_value is not null then 'Y' else 'N' end as is_addition,
null  as amtpct,
'N/A' as deductionind,
b.createddate::date as created_transaction_date,
b.actual_date as change_date,
b.ftcr_fintranscategoryid,
b.contraind,
'n/a' as instalmentnum,
'n/a' as instalmenttype,
b.linestatus,
case when b.ledgertransstatus = 'UnAllocated' then 0 else b.origamt end origamt, -- set to zero anything that is "deallocated"
case when b.ledgertransstatus = 'UnAllocated' then 0 else b.ledgeramt end ledgeramt, -- set to zero antthing tht is "deallocated"
b.origccyiso,
b.ledgerccyiso,
b.synd,
b.producingteam,
b.inceptiondate,
b.expirydate,
b.yoa,
b.assured,
b.assured_addr_id,
b.reassured,
b.client,
b.producer,
b.placer,
b.retailer,
b.class1,
b.class2,
b.class3,
b.class4


from {{ref('stg_fct_booked_b_usmtrans')}} b
 
    left join {{ref('addition_lookup')}} a on 
        b.category = a.category_lookup_value -- this looks up to a seed table to identify specific categories which qualify as is_addition
--       
--where 
--   u.linestatus in ('Written','Signed','Canc') --deliberate omission of this as there are clearly signing messages received when the policy is at another state

),
  get_change as (
  select *,
  conditional_change_event(change_key) over (partition by nk_transaction order by change_date) as change_event
    from cte_nk_deriv
  order by change_date
    )
  ,lag_change as (
 
    select * 
    ,ifnull(lag(change_event) over (partition by nk_transaction order by change_date),-1) as lag_change_val
    
    from 
    get_change
    
   ),
   lag_vals as (
   
   select 
   
   c.*,
   case when change_date > created_transaction_date then change_date else created_transaction_date end as transaction_date,
   ifnull(lag(origamt) over (partition by nk_transaction order by lag_change_val),0) as lagged_origamt,
   ifnull(lag(ledgeramt) over (partition by nk_transaction order by lag_change_val),0) as lagged_ledgeramt

   from lag_change c
   where c.change_event != c.lag_change_val
   
     )
,
     
   calc_trans_val as (
     
     
   select 
     
   *
   ,origamt - lagged_origamt as transaction_orig_amt ---so it doesn't look like they update the financial amounts POST entry.They contra them. This overall lag logic is just a small protection. However, the transaction_key is left without a date grain so if they do update a value in the future, we'll fail the test and need to extend this stub logic.
   ,ledgeramt - lagged_ledgeramt as transaction_ledger_amt -- FURTHER TO THE ABOVE A) THE STATUS CHANGES SO THIS TECHNICALLY QUALIFIES AS A CHANGE OF VALUE TO MAINTAIN THE HISTORY.
   
   from lag_vals

     )

    select 
       
   sha2(nk_transaction::text||change_key::text) as transaction_key
    ,*
    from calc_trans_val
    where transaction_orig_amt != 0
    or transaction_ledger_amt != 0 