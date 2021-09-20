

{{ config(materialized='table') }}

with cte_source as
(
select actual_date,policyid,policyorgtype,orgid
from {{ref('stg_policy_organisations')}}
  where orgorder = 1  --this seems to resolve a fanning issue on the organisations. The MAX below is because I want a pivot by role and I need something in the aggregate.
)

select * 
  from cte_source
    pivot(max(orgid) for policyorgtype in ('ASSURED','REASSURED','CLIENT'))
      as p (actual_date,policyid,ASSURED,REASSURED,CLIENT)
  order by policyid,actual_date


-- However, not all policies have a reassured and / or assured - they should all have that I think? It may be we've excluded something with orgorder = 1 
-- above. Something to park. Checked the assured / reassureds against the Excel datamart and they seemed right. Also works if they shift the assured (which sometimes happens)
