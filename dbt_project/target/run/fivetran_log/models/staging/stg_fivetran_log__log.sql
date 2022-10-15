

  create or replace table `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__log`
  
  
  OPTIONS()
  as (
    with log as (

    select * 
    from `fivetran-hands-on-lab`.`fivetran_log`.`log`
),

fields as (

    select
        id as log_id, 
        cast(time_stamp as 
    timestamp
) as created_at,
        connector_id, -- Note: the connector_id column used to erroneously equal the connector_name, NOT its id.
        case when transformation_id is not null and event is null then 'TRANSFORMATION'
        else event end as event_type, 
        message_data,
        case 
        when transformation_id is not null and message_data like '%has succeeded%' then 'transformation run success'
        when transformation_id is not null and message_data like '%has failed%' then 'transformation run failed'
        else message_event end as event_subtype,
        transformation_id
    from log
)

select * 
from fields
  );
  