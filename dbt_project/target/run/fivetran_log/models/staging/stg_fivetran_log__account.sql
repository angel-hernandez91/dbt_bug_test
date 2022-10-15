

  create or replace table `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__account`
  
  
  OPTIONS()
  as (
    with account as (
    
    select * 
    from `fivetran-hands-on-lab`.`fivetran_log`.`account`
),

fields as (

    select
        id as account_id,
        country,
        cast(created_at as 
    timestamp
) as created_at,
        name as account_name,
        status
    from account
)

select * 
from fields
  );
  