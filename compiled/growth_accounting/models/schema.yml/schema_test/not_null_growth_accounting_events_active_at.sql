
    
    



select count(*) as validation_errors
from "dev"."public"."growth_accounting_events"
where active_at is null


