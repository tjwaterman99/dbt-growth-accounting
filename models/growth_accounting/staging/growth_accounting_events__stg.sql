select
    '{{ var("object_type") }}'::varchar as "object_type",
    {{ var('object_id_field') }}::varchar as "object_id",
    {{ var('timestamp_field') }}::date as "active_at"
from {{ var('tablename') }}
where {{ var('object_id_field') }} is not null
  and {{ var('timestamp_field') }} is not null