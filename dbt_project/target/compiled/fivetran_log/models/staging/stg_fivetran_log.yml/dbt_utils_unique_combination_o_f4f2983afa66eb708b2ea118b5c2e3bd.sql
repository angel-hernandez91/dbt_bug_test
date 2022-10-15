





with validation_errors as (

    select
        active_volume_id, destination_id
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__active_volume`
    group by active_volume_id, destination_id
    having count(*) > 1

)

select *
from validation_errors


