





with validation_errors as (

    select
        connector_name, destination_id
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__connector`
    group by connector_name, destination_id
    having count(*) > 1

)

select *
from validation_errors


