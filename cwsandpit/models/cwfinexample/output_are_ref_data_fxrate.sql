
with 
cte_discodes as (
-- fetch all destination codes

    select distinct 
    exchange_date,dst_isochar_code 
    from fxrate
),

cte_onerates as (
-- generate some rows that have one to one rates for all currencies

    select exchange_date,dst_isochar_code as src_isochar_code, dst_isochar_code, 1 as fx_rate
    from cte_discodes
    union all
    select distinct exchange_date,'USD' as src_isochar_code,'USD' dst_isochar_code, 1 as fx_rate
    from cte_discodes
),

crosser as (
-- create a union of entity codes and add an exchange rate type
-- Note: PNL removed 24/01/2022

    select '120' as legal_entity,'CLOSING' as rtype
    --union select '120' as legal_entity,'PNL' as rtype
    union select '130' as legal_entity,'CLOSING' as rtype
    --union select '130' as legal_entity,'PNL' as rtype
),
allrates as (
-- intermediate set that contains all provided rates and the manufactured 1-to-1 in a union

    select exchange_date,src_isochar_code,dst_isochar_code,fx_rate from fxrate
    union all select * from  cte_onerates

), cte_entity_all_rates as (
-- cross that with the entity cte to replicate a rate for each entity

select 

-- we receive fx rates once per month for the end of the month
-- SMEs confirmed that they would like those rates projected into the following month for use
-- (i.e. the rate received on 2021-01-31 applies 2021-02-01 thru to 2021-02-28)

exc.exchange_date ,                                      --as SRF_FR_FXRATE_DATE,
dateadd('month',1,date_trunc('month',exc.exchange_date::date))                   as nxt_start,
year(nxt_start)                                         as year_exchange_date,
month(nxt_start)                                        as month_exchange_date,
src_isochar_code                                        as SRF_FR_CU_CURRENCY_NUMER_CODE,
exc.dst_isochar_code                                    as SRF_FR_CU_CURRENCY_DENOM_CODE,
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
1                                                   as LPG_ID

from 

allrates exc

    cross join crosser c

)



select 

d.date                                                  as SRF_FR_FXRATE_DATE,
SRF_FR_CU_CURRENCY_NUMER_CODE,
d.date                                                  as SRF_FR_FXRATE_DATE_FWD,
SRF_FR_CU_CURRENCY_DENOM_CODE,
SRF_FR_SI_SYS_INST_CODE,
SRF_FR_FX_RATE,
SRF_FR_PL_PARTY_LEGAL_CODE,
SRF_FR_SOURCE_INPUT_TIME,
SRF_FR_RTY_RATE_TYPE_ID,
EVENT_ERROR_STRING,
PROCESS_ID,
SUB_SYSTEM_ID,
MESSAGE_ID,
REMITTING_SYSTEM_ID,
LPG_ID

from cte_entity_all_rates e

    inner join dim_dates d 
        on d.calendar_year = e.year_exchange_date
        and d.calendar_month_of_year = e.month_exchange_date

        