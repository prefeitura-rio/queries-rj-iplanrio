{{
    config(
        alias="service_catalog",
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge"
    )
}}
SELECT
    cast(Id AS string) AS id,
    cast(OwnerId AS string) AS owner_id,
    cast(IsDeleted AS boolean) AS is_deleted,
    cast(Name AS string) AS name,
    cast(RecordTypeId AS string) AS record_type_id,
    cast(CreatedDate AS timestamp) AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    cast(LastModifiedDate AS timestamp) AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    cast(SystemModstamp AS timestamp) AS system_modstamp,
    cast(LastViewedDate AS timestamp) AS last_viewed_date,
    cast(LastReferencedDate AS timestamp) AS last_referenced_date,
    cast(IsActive__c AS boolean) AS is_active,
    cast(Available_For_Citizen__c AS boolean) AS available_for_citizen,
    cast(Slug__c AS string) AS slug,
    cast(Ombudsman_exclusive__c AS boolean) AS ombudsman_exclusive,
    cast(Is_LAI__c AS boolean) AS is_lai,
    cast(Service_Hierarchy__c AS string) AS service_hierarchy,
    cast(Internal_Only__c AS boolean) AS internal_only,
    cast(SGRC_Legacy_CRM_Subtype_Id__c AS string) AS sgrc_legacy_crm_subtype_id,
    cast(Unique_Key__c AS string) AS unique_key,
    cast(Theme__c AS string) AS theme,
    cast(Subtheme__c AS string) AS subtheme,
    cast(Theme_formula__c AS string) AS theme_formula,
    cast(Subtheme_formula__c AS string) AS subtheme_formula,
    cast(Service_Name__c AS string) AS service_name,
    cast(Service_hyperlink_formula__c AS string) AS service_hyperlink_formula,
    cast(Responsible_Org_Unit__c AS string) AS responsible_org_unit,
    cast(Record_Type_Developer_Name__c AS string) AS record_type_developer_name,
    cast(_airbyte_extracted_at AS datetime) AS _airbyte_extracted_at
FROM {{source ("brutos_czrm_salesforce_staging","Service_Catalog__c")}}
    {% if is_incremental() %}
        where SystemModstamp > (select max(system_modstamp) from {{ this }})
    {% endif %}