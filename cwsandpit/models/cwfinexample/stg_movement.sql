-- Movement. Movement is transactional BUT STORES SNAPSHOTS AND TRANSACTIONAL
-- The assumption here is that TEMPORAL history is not technically required.

-- (This assumption is incorrect - see later queries - in a minority of the required data)



--SOURCE: CURATED SNAPSHOT TABLE


select * 
from movement