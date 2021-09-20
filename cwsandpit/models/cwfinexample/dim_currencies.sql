---SOURCE: A COPY OF CURRENCY_DIM FROM REPORTING_COMMON


select *
from currency_dim

union 
select 
'n/a' as currency_name,
'n/a' as isochar_code,
'1900-01-01'::date as date_started,
'2099-01-01'::date as date_withdrawn,
1 as current_row_indicator,
'2019-12-31'::date as effective_from,
'2099-01-01'::date as effective_to,
sha2('n/a') as currency_key

union 
select 
'Unknown' as currency_name,
'Unknown' as isochar_code,
'1900-01-01'::date as date_started,
'2099-01-01'::date as date_withdrawn,
1 as current_row_indicator,
'2019-12-31'::date as effective_from,
'2099-01-01'::date as effective_to,
sha2('Unknown') as currency_key