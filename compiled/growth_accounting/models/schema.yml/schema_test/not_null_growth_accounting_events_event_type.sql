
    
    



select count(*) as validation_errors
from "dev"."public"."growth_accounting_events"
where event_type is null


