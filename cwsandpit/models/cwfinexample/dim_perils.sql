

-- TYPE 1 THIS ONE BECAUSE I DON'T THINK IT'S NECESSARY TO CARE ABOUT THE HISTORY OF THE PERIL DIMENSION

with cte_alldays as (
select 
p.actual_date
,p.perilid
,p.peril
,p.perildsc
,pg.uwdivision
,pg.perilgroup
from stg_peril p
    join stg_perilgroupdetail pgd 
        on p.perilid = pgd.perilid
        and p.actual_date = pgd.actual_date
    join stg_perilgroup pg
        on pgd.perilgroupid = pg.perilgroupid
        and pgd.actual_date = pg.actual_date

),

latest_day as (

    select max(actual_date) as latest_date
    from cte_alldays

)

select distinct
sha2(v.peril::text) as peril_key
,v.peril::text as peril_nk
--case when v.valid_from = '2019-12-31' then '1901-01-01' else v.valid_from end as effective_from,
--case when v.valid_to = '9999-12-31' then v.valid_to else dateadd(day,-1,v.valid_to) end as effective_to,
--case when v.valid_to = '9999-12-31' then 1 else 0 end as is_current_row,
,'n/a' as change_key
,'eclipse' as source_system
,v.peril as peril_code
,v.perildsc as peril_description
,v.uwdivision as uw_division
,v.perilgroup as peril_group

from 
 cte_alldays v
    join latest_day d on   
        v.actual_date = d.latest_date

union all

select 
sha2('Unknown') as peril_key
,'Unknown' as peril_nk
,'n/a' as change_key
,'n/a' as source_system
,'Unknown' as peril_code
,'Unknown' as peril_description
,'Unknown' as uw_division
,'Unknown' as peril_group

union all

select 
sha2('n/a') as peril_key
,'n/a' as peril_nk
,'n/a' as change_key
,'n/a' as source_system
,'n/a' as peril_code
,'n/a' as peril_description
,'n/a' as uw_division
,'n/a' as peril_group
