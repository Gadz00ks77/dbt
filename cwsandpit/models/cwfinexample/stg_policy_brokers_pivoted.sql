{{ config(materialized='table') }}

--Flattens to one row per policy per day.


with cte_source as
(
select actual_date,policyid,lloydsbrokerid,brokerrole
from {{ref('stg_policy_brokers')}}
  where ledgerbroker = 'Y' --mostly resolves fan to one broker per policy (once pivoted below)
)

select * 
  from cte_source
    pivot(max(lloydsbrokerid) for brokerrole in ('RETAILER','PRODUCER','PLACER')) --max'd here because there are two lines with two ledgerbroker = 'Y' Placers. Something to sort later. May be array key.
      as p (actual_date,policyid,RETAILER,PRODUCER,PLACER)
  order by policyid,actual_date

