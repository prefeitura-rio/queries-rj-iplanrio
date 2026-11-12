{% macro raw_czrm__service_catalog(source_name) %}

select
    cast(Id as string)                              as id,
    cast(OwnerId as string)                         as owner_id,
    cast(IsDeleted as boolean)                      as is_deleted,
    cast(Name as string)                            as name,
    cast(RecordTypeId as string)                    as record_type_id,
    datetime(CreatedDate, 'America/Sao_Paulo')                  as created_date,
    cast(CreatedById as string)                     as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')             as last_modified_date,
    cast(LastModifiedById as string)                as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')               as system_modstamp,
    datetime(LastViewedDate, 'America/Sao_Paulo')               as last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo')           as last_referenced_date,
    cast(IsActive__c as boolean)                    as is_active,
    cast(Available_For_Citizen__c as boolean)       as available_for_citizen,
    cast(Slug__c as string)                         as slug,
    cast(Ombudsman_exclusive__c as boolean)         as ombudsman_exclusive,
    cast(Is_LAI__c as boolean)                      as is_lai,
    cast(Service_Hierarchy__c as string)            as service_hierarchy,
    cast(Internal_Only__c as boolean)               as internal_only,
    cast(SGRC_Legacy_CRM_Subtype_Id__c as string)   as sgrc_legacy_crm_subtype_id,
    cast(Unique_Key__c as string)                   as unique_key,
    cast(Theme__c as string)                        as theme,
    cast(Subtheme__c as string)                     as subtheme,
    cast(Theme_formula__c as string)                as theme_formula,
    cast(Subtheme_formula__c as string)             as subtheme_formula,
    cast(Service_Name__c as string)                 as service_name,
    cast(Service_hyperlink_formula__c as string)    as service_hyperlink_formula,
    cast(Responsible_Org_Unit__c as string)         as responsible_org_unit,
    cast(Record_Type_Developer_Name__c as string)   as record_type_developer_name,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo')         as _airbyte_extracted_at
from {{ source(source_name, "Service_Catalog__c") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
