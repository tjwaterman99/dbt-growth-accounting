select
    '{{ var("object_type") }}'::varchar as "object_type",
    '{{ var("event_type") }}'::varchar as "event_type",    
    {{ var('object_id_field') }}::varchar as "object_id",
    {{ var('event_timestamp_field') }}::date as "active_at"
from {{ var('tablename') }}
where {{ var('object_id_field') }} is not null
  and {{ var('event_timestamp_field') }} is not null