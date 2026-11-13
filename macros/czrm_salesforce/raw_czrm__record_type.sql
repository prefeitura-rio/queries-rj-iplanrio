{% macro raw_czrm__record_type (source_name) %}
SELECT
    cast(Id AS string) AS id,
    cast(Name AS string) AS name,
    cast(DeveloperName AS string) AS developer_name,
    cast(NamespacePrefix AS string) AS namespace_prefix,
    cast(Description AS string) AS description,
    cast(BusinessProcessId AS string) AS business_process_id,
    cast(SobjectType AS string) AS sobject_type,
    cast(IsActive AS boolean) AS is_active,
    cast(IsPersonType AS boolean) AS is_person_type,
    cast(CreatedById AS string) AS created_by_id,
    datetime(CreatedDate, 'America/Sao_Paulo') AS created_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo') AS last_modified_date,
    datetime(SystemModstamp, 'America/Sao_Paulo') AS system_modstamp,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {{source (source_name, "RecordType")}} {% if is_incremental () %}
WHERE
    datetime(SystemModstamp, 'America/Sao_Paulo') >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}