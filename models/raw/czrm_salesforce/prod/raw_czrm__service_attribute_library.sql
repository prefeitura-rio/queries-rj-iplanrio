{{
    config(
        alias="service_attribute_library",
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
    cast(CreatedDate AS timestamp) AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    cast(LastModifiedDate AS timestamp) AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    cast(SystemModstamp AS timestamp) AS system_modstamp,
    cast(LastViewedDate AS timestamp) AS last_viewed_date,
    cast(LastReferencedDate AS timestamp) AS last_referenced_date,
    cast(Field_Label_Name__c AS string) AS field_label_name,
    cast(Integracao_Ativa__c AS boolean) AS integracao_ativa,
    cast(Field_Type__c AS string) AS field_type,
    cast(Unique_Key__c AS string) AS unique_key,
    cast(Validation_Regex__c AS string) AS validation_regex,
    cast(Validation_Error_Message__c AS string) AS validation_error_message,
    cast(Global_Help_Text__c AS string) AS global_help_text,
    cast(IsActive__c AS boolean) AS is_active,
    cast(Valores_cadastrados__c AS numeric) AS valores_cadastrados,
    cast(Form_Section__c AS string) AS form_section,
    cast(Custom_LWC_Name__c AS string) AS custom_lwc_name,
    cast(_airbyte_extracted_at AS datetime) AS _airbyte_extracted_at
FROM {{source ("brutos_czrm_salesforce","Service_Attribute_Library__c")}}
    {% if is_incremental() %}
        where SystemModstamp > (select max(system_modstamp) from {{ this }})
    {% endif %}