
-- Create a "as at day" view of the address table
-- THESE STAGE QUERIES (UNLESS THE REF SYNTAX INDICATES OTHERWISE) ARE MOSTLY USING THE 
-- SNAPSHOT CURATED TABLES IN DU PROD. e.g. for this one;
-- "DU_PROD"."CURATED_WNS_ECLIPSE_AND_PREQUEL_EXPORT"."ADDR_SNAPSHOT_CURATED_WNS_ECLIPSE_AND_PREQUEL_EXPORT"
-- I shifted the ones I needed over to the Arch db on Dedicted Snowflake hence the different name.


--SOURCE: CURATED SNAPSHOT TABLE



{{ config(materialized='table') }}
with cte_date_all as
(
  select 
    to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) as actual_date 
  from dim_dates
  where to_date(substr(date,1,4)||'-'||substr(date,6,2)||'-'||substr(date,9,2)) <= current_date
),
eff_range_source as(
    select 
    p.*
    ,case when actual_date >= valid_from and actual_date <= valid_to then 1 else 0 end as in_date
    ,actual_date
    from 
    addr p
    cross join cte_date_all d

)
select *

    from eff_range_source p
    where in_date = 1