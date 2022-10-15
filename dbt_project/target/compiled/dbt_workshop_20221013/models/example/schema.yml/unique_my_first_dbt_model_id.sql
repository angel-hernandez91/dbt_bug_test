
    
    

with dbt_test__target as (

  select id as unique_field
  from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev`.`my_first_dbt_model`
  where id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


