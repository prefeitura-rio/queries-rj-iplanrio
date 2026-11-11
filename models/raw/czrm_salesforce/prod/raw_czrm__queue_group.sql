{{ config(alias="queue_group") }}

select
    cast(Id as string)                      as id,
    cast(Name as string)                    as name,
    cast(DeveloperName as string)           as developer_name,
    cast(RelatedId as string)               as related_id,
    cast(Type as string)                    as `type`,
    cast(Email as string)                   as email,
    cast(QueueRoutingConfigId as string)    as queue_routing_config_id,
    cast(OwnerId as string)                 as owner_id,
    cast(DoesSendEmailToMembers as boolean) as does_send_email_to_members,
    cast(DoesIncludeBosses as boolean)      as does_include_bosses,
    cast(CreatedDate as timestamp)          as created_date,
    cast(CreatedById as string)             as created_by_id,
    cast(LastModifiedDate as timestamp)     as last_modified_date,
    cast(LastModifiedById as string)        as last_modified_by_id,
    cast(SystemModstamp as timestamp)       as system_modstamp,
    cast(Description as string)             as description,
    cast(_airbyte_extracted_at as datetime) as _airbyte_extracted_at
from {{ source("brutos_czrm_salesforce", "Group") }}
