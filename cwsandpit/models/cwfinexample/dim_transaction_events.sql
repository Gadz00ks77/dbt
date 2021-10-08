select 

sha2(te.transaction_event::text||te.transaction_sub_event::text||'reference') as transaction_event_key,
'reference' as source_system,
te.transaction_event_code,
te.transaction_event,
te.transaction_sub_event

from {{ref('stg_transaction_event')}} te

