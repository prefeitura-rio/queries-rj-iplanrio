{% macro raw_czrm__queue_group (source_name) %}
SELECT
    cast(Id AS string) AS id,
    cast(Name AS string) AS name,
    cast(DeveloperName AS string) AS developer_name,
    cast(RelatedId AS string) AS related_id,
    cast(Type AS string) AS group_type,
    cast(Email AS string) AS email,
    cast(QueueRoutingConfigId AS string) AS queue_routing_config_id,
    cast(OwnerId AS string) AS owner_id,
    cast(DoesSendEmailToMembers AS boolean) AS does_send_email_to_members,
    cast(DoesIncludeBosses AS boolean) AS does_include_bosses,
    datetime(CreatedDate, 'America/Sao_Paulo') AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo') AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo') AS system_modstamp,
    cast(Description AS string) AS description,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {{source (source_name, "Group")}} {% if is_incremental () %}
WHERE
    SystemModstamp >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}