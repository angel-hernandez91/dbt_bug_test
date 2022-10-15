

  create or replace table `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__usage_cost`
  
  
  OPTIONS()
  as (
    

select
    cast(null as 
    string
) as destination_id,
    cast(null as 
    string
) as measured_month,
    cast(null as 
    int64
) as dollars_spent


  );
  