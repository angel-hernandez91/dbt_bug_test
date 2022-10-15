with connector as (

    select * 
    from `fivetran-hands-on-lab`.`dbt_workshop_20221013_dev_stg_fivetran_log`.`stg_fivetran_log__connector_tmp`
),

fields as (
    select
        
    
    
    _fivetran_deleted
    
 as 
    
    _fivetran_deleted
    
, 
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    connecting_user_id
    
 as 
    
    connecting_user_id
    
, 
    
    
    connector_id
    
 as 
    
    connector_id
    
, 
    
    
    connector_name
    
 as 
    
    connector_name
    
, 
    cast(null as 
    string
) as 
    
    connector_type
    
 , 
    
    
    connector_type_id
    
 as 
    
    connector_type_id
    
, 
    
    
    destination_id
    
 as 
    
    destination_id
    
, 
    
    
    paused
    
 as 
    
    paused
    
, 
    cast(null as 
    int64
) as 
    
    service_version
    
 , 
    
    
    signed_up
    
 as 
    
    signed_up
    



        ,row_number() over ( partition by connector_name, destination_id order by _fivetran_synced desc ) as nth_last_record
    from connector
),

final as (

    select 
        connector_id,
        connector_name,
        coalesce(connector_type_id, connector_type) as connector_type,
        destination_id,
        connecting_user_id,
        paused as is_paused,
        signed_up as set_up_at
    from fields

    -- Only look at the most recent one
    where nth_last_record = 1
)

select * 
from final