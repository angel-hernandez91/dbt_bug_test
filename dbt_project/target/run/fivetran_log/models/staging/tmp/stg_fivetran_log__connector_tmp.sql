

  create or replace view `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__connector_tmp`
  OPTIONS()
  as select *
from `fivetran-hands-on-lab`.`fivetran_log`.`connector`;

