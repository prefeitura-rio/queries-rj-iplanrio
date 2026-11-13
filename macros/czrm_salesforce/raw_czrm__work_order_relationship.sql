{% macro raw_czrm__work_order_relationship(source_name) %}
SELECT
    cast(Id AS string) AS id,
    cast(OwnerId AS string) AS owner_id,
    cast(IsDeleted AS boolean) AS is_deleted,
    cast(Name AS string) AS name,
    datetime(CreatedDate, 'America/Sao_Paulo') AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo') AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo') AS system_modstamp,
    cast(Case__c AS string) AS case_id,
    cast(Tipo__c AS string) AS tipo,
    cast(WO_Origem__c AS string) AS wo_origem,
    cast(WO_Relacionada__c AS string) AS wo_relacionada,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {{source(source_name, "WorkOrder_Relationship__c")}} {% if is_incremental() %}
WHERE
    datetime(SystemModstamp, 'America/Sao_Paulo') >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}