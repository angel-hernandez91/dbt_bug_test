

  create or replace table `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_fivetran_log`.`fivetran_log__schema_changelog`
  
  
  OPTIONS()
  as (
    with schema_changes as (

    select *
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__log`

    where event_subtype in ('create_table', 'alter_table', 'create_schema', 'change_schema_config')
),

connector as (

    select *
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_fivetran_log`.`fivetran_log__connector_status`
),

add_connector_info as (

    select 
        schema_changes.*,
        connector.connector_name,
        connector.destination_id,
        connector.destination_name

    from schema_changes join connector using(connector_id)
),

final as (

    select
        connector_id,
        connector_name,
        destination_id,
        destination_name,
        created_at,
        event_subtype,
        message_data,

        case 
        when event_subtype = 'alter_table' then 

 
  json_extract_scalar(message_data, '$.table')

 
        when event_subtype = 'create_table' then 

 
  json_extract_scalar(message_data, '$.name')

 
        else null end as table_name,

        case 
        when event_subtype = 'create_schema' or event_subtype = 'create_table' then 

 
  json_extract_scalar(message_data, '$.schema')

 
        else null end as schema_name
    
    from add_connector_info
)

select * from final
order by created_at desc, connector_id
  );
  