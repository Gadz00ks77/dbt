
with 
cte_discodes as (

    select distinct 
    exchange_date,dst_isochar_code 
    from fxrate
),

cte_onerates as (

    select exchange_date,dst_isochar_code as src_isochar_code, dst_isochar_code, 1 as fx_rate
    from cte_discodes
    union all
    select distinct exchange_date,'USD' as src_isochar_code,'USD' dst_isochar_code, 1 as fx_rate
    from cte_discodes
),

crosser as (

    select '120' as legal_entity,'CLOSING' as rtype
    union select '120' as legal_entity,'PNL' as rtype
    union select '130' as legal_entity,'CLOSING' as rtype
    union select '130' as legal_entity,'PNL' as rtype
),
allrates as (

    select exchange_date,src_isochar_code,dst_isochar_code,fx_rate from fxrate
    union all select * from  cte_onerates

)
select 

exc.exchange_date                                       as SRF_FR_FXRATE_DATE,
src_isochar_code                                        as SRF_FR_CU_CURRENCY_NUMER_CODE,
exc.exchange_date                                       as SRF_FR_FXRATE_DATE_FWD,
exc.dst_isochar_code                                        as SRF_FR_CU_CURRENCY_DENOM_CODE,
'Client Static'                                         as SRF_FR_SI_SYS_INST_CODE,
fx_rate::number(28,8)                                   as SRF_FR_FX_RATE,
c.legal_entity                                          as SRF_FR_PL_PARTY_LEGAL_CODE,
null                                                    as SRF_FR_SOURCE_INPUT_TIME,
c.rtype                                                 as SRF_FR_RTY_RATE_TYPE_ID,
null                                                    as EVENT_ERROR_STRING,
null                                                    as PROCESS_ID,
null                                                    as SUB_SYSTEM_ID,
null                                                    as MESSAGE_ID,
null                                                    as REMITTING_SYSTEM_ID,
null                                                    as LPG_ID

from 

allrates exc

    cross join crosser c