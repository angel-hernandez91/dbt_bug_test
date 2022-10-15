





with validation_errors as (

    select
        connector_id, destination_id, message_data, created_at
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_fivetran_log`.`fivetran_log__schema_changelog`
    group by connector_id, destination_id, message_data, created_at
    having count(*) > 1

)

select *
from validation_errors


