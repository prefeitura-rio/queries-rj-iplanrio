{% macro raw_czrm__record_type(source_name) %}

select
    cast(Id as string)                      as id,
    cast(Name as string)                    as name,
    cast(DeveloperName as string)           as developer_name,
    cast(NamespacePrefix as string)         as namespace_prefix,
    cast(Description as string)             as description,
    cast(BusinessProcessId as string)       as business_process_id,
    cast(SobjectType as string)             as sobject_type,
    cast(IsActive as boolean)               as is_active,
    cast(IsPersonType as boolean)           as is_person_type,
    cast(CreatedById as string)             as created_by_id,
    datetime(CreatedDate, 'America/Sao_Paulo')          as created_date,
    cast(LastModifiedById as string)        as last_modified_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')     as last_modified_date,
    datetime(SystemModstamp, 'America/Sao_Paulo')       as system_modstamp,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') as _airbyte_extracted_at
from {{ source(source_name, "RecordType") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
