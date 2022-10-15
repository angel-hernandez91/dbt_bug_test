
    
    

with dbt_test__target as (

  select account_id as unique_field
  from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__account`
  where account_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


