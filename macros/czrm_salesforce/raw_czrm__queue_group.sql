{% macro raw_czrm__queue_group(source_name) %}

select
    cast(Id as string)                      as id,
    cast(Name as string)                    as name,
    cast(DeveloperName as string)           as developer_name,
    cast(RelatedId as string)               as related_id,
    cast(Type as string)                    as group_type,
    cast(Email as string)                   as email,
    cast(QueueRoutingConfigId as string)    as queue_routing_config_id,
    cast(OwnerId as string)                 as owner_id,
    cast(DoesSendEmailToMembers as boolean) as does_send_email_to_members,
    cast(DoesIncludeBosses as boolean)      as does_include_bosses,
    datetime(CreatedDate, 'America/Sao_Paulo')            as created_date,
    cast(CreatedById as string)             as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')       as last_modified_date,
    cast(LastModifiedById as string)        as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')         as system_modstamp,
    cast(Description as string)             as description,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') as _airbyte_extracted_at
from {{ source(source_name, "Group") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
