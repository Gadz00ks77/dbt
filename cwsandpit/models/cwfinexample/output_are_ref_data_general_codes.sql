


select *
from
{{ref('output_are_ref_data_cob')}}

union all 
select *
from
{{ref('output_are_ref_data_contracttype')}}

union all
select *
from
{{ref('output_are_ref_data_placingbasis')}}

union all
select *
from
{{ref('output_are_ref_data_reservingsegment')}}


union all
select *
from
{{ref('output_are_ref_data_targetmarket')}}






