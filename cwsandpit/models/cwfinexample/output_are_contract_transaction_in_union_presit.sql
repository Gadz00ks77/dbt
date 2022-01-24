


with cte_out as (
select *
    from {{ref('output_are_contract_transaction_in_claims_presit')}}

union all 

select *
    from {{ref('output_are_contract_transaction_in_written_presit')}}

)


select *
from cte_out
where contract_clicode not in 
(
    'DA298S21D000',
'DA298S21C000',
'DA298S21F000',
'DA086G20A000',
'DA298S21B000',
'DA298S21E000',
'DA298S21A000'
)

--excluded due to some weirdy line status stuff that I think is caused by people buggering up the entity reg.