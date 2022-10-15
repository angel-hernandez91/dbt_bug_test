





with validation_errors as (

    select
        log_id, created_at
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__log`
    group by log_id, created_at
    having count(*) > 1

)

select *
from validation_errors


