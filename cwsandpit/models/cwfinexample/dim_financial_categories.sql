select distinct --this isn't right as some attributes on transactions DO change... but not that we're interested in for now so this is faked.

sha2(c.fintranscategoryid::text||'9999-12-31'::text||'eclipse') as financial_category_key,
'eclipse' as source_system,
c.fintranscategoryid::text as nk_financial_category,
'1901-01-01'::date as valid_from,
'9999-12-31'::date as valid_to,
ifnull(ft.category,c.description) as sub_category,
c.woodwardtype as category,
c.woodwardtypegroup as parent_category

from {{ref('stg_financial_categories')}} c

 left join fintranscategory ft on
    ft.fintranscategoryid = c.fintranscategoryid

union 

select 
sha2('n/a') as financial_category_key,
'n/a' as source_system,
'n/a' as nk_financial_category,
'1901-01-01'::date as valid_from,
'9999-12-31'::date as valid_to,
'n/a' as sub_category,
'n/a' as category,
'n/a' as parent_category

union 

select 
sha2('unknown') as financial_category_key,
'n/a' as source_system,
'unknown' as nk_financial_category,
'1901-01-01'::date as valid_from,
'9999-12-31'::date as valid_to,
'unknown' as sub_category,
'unknown' as category,
'unknown' as parent_category