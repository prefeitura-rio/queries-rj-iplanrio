{% macro raw_czrm__work_order_relationship(source_name) %}

select
    cast(Id as string)                      as id,
    cast(OwnerId as string)                 as owner_id,
    cast(IsDeleted as boolean)              as is_deleted,
    cast(Name as string)                    as name,
    datetime(CreatedDate, 'America/Sao_Paulo')          as created_date,
    cast(CreatedById as string)             as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')     as last_modified_date,
    cast(LastModifiedById as string)        as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')       as system_modstamp,
    cast(Case__c as string)                 as case_id,
    cast(Tipo__c as string)                 as tipo,
    cast(WO_Origem__c as string)            as wo_origem,
    cast(WO_Relacionada__c as string)       as wo_relacionada,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') as _airbyte_extracted_at
from {{ source(source_name, "WorkOrder_Relationship__c") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
