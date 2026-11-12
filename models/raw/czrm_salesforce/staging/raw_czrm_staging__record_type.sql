{{
    config(
        alias="record_type",
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge"
    )
}}
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
    cast(CreatedDate AS timestamp) AS created_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    cast(LastModifiedDate AS timestamp) AS last_modified_date,
    cast(SystemModstamp AS timestamp) AS system_modstamp,
    cast(_airbyte_extracted_at AS datetime) AS _airbyte_extracted_at
FROM {{source ("brutos_czrm_salesforce_staging", "RecordType")}}
    {% if is_incremental() %}
        where SystemModstamp > (select max(system_modstamp) from {{ this }})
    {% endif %}