
with cte_1 as (

select * 
from {{ref('output_are_contract_transaction_in')}}
-- union all
-- select * 
-- from {{ref('output_are_manual_ibnr')}}
-- union all
-- select * 
-- from {{ref('output_are_manual_mm')}}
-- union all
-- select * 
-- from {{ref('output_are_manual_pipeline')}}
-- union all
-- select * 
-- from {{ref('output_are_manual_rip')}}

) 

select 


t._nk_transin,
t._snapshot_date,
t.EVENT_ERROR_STRING, 
t.NO_RETRIES,
t.STAN_RULE_IDENT,
t.PROCESS_ID,
t.SUB_SYSTEM_ID,
t.MESSAGE_ID,
t.REMITTING_SYSTEM_ID,
t.SOURCE_TRAN_NO,
t.SOURCE_TRAN_VER,
t.EVENT_AUDIT_ID,
t.BUSINESS_DATE,
t.CONTRACT_SYS_INST_CODE,
t.AE_ACC_EVENT_TYPE_ID,
t.AE_SUB_EVENT_ID,
t.AE_ACCOUNTING_DATE, -- Movement Date
t.AE_POS_NEG,
t.AE_DIMENSION_1, 
t.AE_DIMENSION_2::int AE_DIMENSION_2, -- Uw Year
t.AE_DIMENSION_3, -- Acc Year
t.AE_DIMENSION_4, -- Mythic "COB" Nonsense
t.AE_DIMENSION_5, -- Target Market
'000' as AE_DIMENSION_6, -- Intercompany
t.AE_DIMENSION_7, -- Placing Basis
t.AE_DIMENSION_8, -- Contract Type
t.AE_DIMENSION_9, 
t.AE_DIMENSION_10,
t.AE_DIMENSION_11, -- More "COB" Stuff
t.AE_DIMENSION_12,
t.AE_DIMENSION_13, -- RI Contract
t.AE_DIMENSION_14, -- Reinsurer
t.AE_DIMENSION_15,
t.DESCRIPTION,
t.PARENT_TRAN_NO,
t.PARENT_TRAN_VER,
t.PARENT_TYPE,
t.CONTRACT_CLICODE, -- Insured Line Eclipse Policy Reference
t.CONTRACT_PART_CLICODE,
t.TRANSACTION_TYPE_CLICODE,
t.BO_BOOK_CLICODE,
t.IPE_ENTITY_CLIENT_CODE,
t.PL_PARTY_LEGAL_CLICODE, -- Legal Entity
t.PBU_PARTY_BUS_CLIENT_CODE,
t.CU_CURRENCY_ISO_CODE, -- Settlement Currency Code
t.GROSS_AMOUNT,
t.NET_AMOUNT,
t.TAX_AMOUNT,
t.TAX_CODE1,
t.TAX_CODE2,
t.TAX_CODE3,
t.INVOICE_DATE,
t.OTHER_DATE1, -- Accounting Date
t.OTHER_DATE2,
t.TOTAL_AMOUNT,
t.CLIENT_TEXT1, -- Transaction Event
t.CLIENT_TEXT2, -- Transaction Sub Event
t.CLIENT_TEXT3, -- Financial Parent Category
t.CLIENT_TEXT4, -- Financial Category
t.CLIENT_TEXT5, -- Financial Sub Category
t.CLIENT_TEXT6, -- Is Addition Marker
t.CLIENT_TEXT7, -- Peril Code (N/A for Premium)
t.CLIENT_TEXT8, --case when change_pol_level = 1 then 'New' Else 'Not New' end as CLIENT_TEXT8, -- New Policy Marker -- deprecated
t.CLIENT_TEXT9, -- Original Currency Code
t.CLIENT_TEXT10,
t.CLIENT_TEXT11,
t.CLIENT_TEXT12,
t.CLIENT_TEXT13,
t.CLIENT_TEXT14,
t.CLIENT_TEXT15,
t.CLIENT_TEXT16,
t.CLIENT_TEXT17,
t.CLIENT_TEXT18,
t.CLIENT_TEXT19,
t.CLIENT_TEXT20, -- Insured Line Status
t.CLIENT_AMOUNT1, -- Settlement Currency Snapshot Amount
t.CLIENT_AMOUNT2, -- Original Currency Snapshot Amount
t.CLIENT_AMOUNT3,
t.CLIENT_AMOUNT4,
t.CLIENT_AMOUNT5,
t.CLIENT_AMOUNT6,
t.CLIENT_AMOUNT7,
t.CLIENT_AMOUNT8,
t.CLIENT_AMOUNT9,
t.CLIENT_AMOUNT10,
t.AE_VALUE_DATE,
t.LOCAL_AMOUNT,
t.LOCAL_CU_CURRENCY_ISO_CODE

 from cte_1 t  
--     left join {{ref('output_are_contract_combined')}} c 
--         on c.contract_clicode = t.contract_clicode

-- where t.client_text2 in ('IBNR','MM')
-- or 
-- c.contract_clicode is not null