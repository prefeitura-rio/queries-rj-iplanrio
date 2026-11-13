{% macro raw_czrm__neighborhood (source_name) %}
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
    datetime(LastViewedDate, 'America/Sao_Paulo') AS last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo') AS last_referenced_date,
    cast(codbairro__c AS string) AS codbairro,
    cast(codbnum__c AS numeric) AS codbnum,
    cast(RA__c AS string) AS ra,
    cast(AP__c AS string) AS ap,
    cast(RP__c AS string) AS rp,
    cast(Subprefeitura__c AS string) AS subprefeitura,
    cast(APS__c AS string) AS aps,
    cast(CRE__c AS string) AS cre,
    cast(Coverage__c AS string) AS coverage,
    cast(Google_Neighborhood_Name__c AS string) AS google_neighborhood_name,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {{source (source_name, "Neighborhood__c")}} {% if is_incremental () %}
WHERE
    SystemModstamp >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}