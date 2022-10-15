

  create or replace table `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__destination`
  
  
  OPTIONS()
  as (
    with destination as (

    select * 
    from `fivetran-hands-on-lab`.`fivetran_log`.`destination`
),

fields as (

    select
        id as destination_id,
        account_id,
        cast(created_at as 
    timestamp
) as created_at,
        name as destination_name,
        region
    from destination
)

select * 
from fields
  );
  