select
    'user'::varchar as "object_type",
    'login'::varchar as "event_type",    
    user_id::varchar as "object_id",
    login_at::date as "active_at"
from "dev"."raw"."growth_accounting_users"
where user_id is not null
  and login_at is not null