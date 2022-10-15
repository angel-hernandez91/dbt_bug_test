
    
    

with dbt_test__target as (

  select unique_table_sync_key as unique_field
  from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_fivetran_log`.`fivetran_log__audit_table`
  where unique_table_sync_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


