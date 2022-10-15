

  create or replace view `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev`.`my_first_dbt_model`
where id = 1;

