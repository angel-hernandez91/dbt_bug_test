





with validation_errors as (

    select
        destination_id, measured_month
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_fivetran_log`.`fivetran_log__usage_mar_destination_history`
    group by destination_id, measured_month
    having count(*) > 1

)

select *
from validation_errors


