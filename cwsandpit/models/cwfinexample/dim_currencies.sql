---SOURCE: A COPY OF CURRENCY_DIM FROM REPORTING_COMMON


 
 select 
 
 currency_name,
 isochar_code,
 date_started,
 date_withdrawn,
 current_row_indicator,
 case when effective_from  = '2019-12-31' then '1901-01-01' else effective_from end as effective_from,
 effective_to,
 currency_key
 
 from currency_dim
 

union 
select 
'n/a' as currency_name,
'n/a' as isochar_code,
'1900-01-01'::date as date_started,
'2099-01-01'::date as date_withdrawn,
1 as current_row_indicator,
'1901-01-01'::date as effective_from,
'2099-01-01'::date as effective_to,
sha2('n/a') as currency_key

union 
select 
'Unknown' as currency_name,
'Unknown' as isochar_code,
'1900-01-01'::date as date_started,
'2099-01-01'::date as date_withdrawn,
1 as current_row_indicator,
'1901-01-01'::date as effective_from,
'2099-01-01'::date as effective_to,
sha2('Unknown') as currency_key