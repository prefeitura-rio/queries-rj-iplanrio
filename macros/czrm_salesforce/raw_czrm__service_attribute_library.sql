{
    % macro raw_czrm__service_attribute_library (source_name) %
}
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
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {
        {
            source (source_name, "Service_Attribute_Library__c")
        }
    } {% if is_incremental () %}
WHERE
    SystemModstamp >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}