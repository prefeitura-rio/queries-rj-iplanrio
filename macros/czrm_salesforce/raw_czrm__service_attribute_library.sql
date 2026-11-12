{% macro raw_czrm__service_attribute_library(source_name) %}

select
    cast(Id as string)                          as id,
    cast(OwnerId as string)                     as owner_id,
    cast(IsDeleted as boolean)                  as is_deleted,
    cast(Name as string)                        as name,
    datetime(CreatedDate, 'America/Sao_Paulo')              as created_date,
    cast(CreatedById as string)                 as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')         as last_modified_date,
    cast(LastModifiedById as string)            as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')           as system_modstamp,
    datetime(LastViewedDate, 'America/Sao_Paulo')           as last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo')       as last_referenced_date,
    cast(Field_Label_Name__c as string)         as field_label_name,
    cast(Integracao_Ativa__c as boolean)        as integracao_ativa,
    cast(Field_Type__c as string)               as field_type,
    cast(Unique_Key__c as string)               as unique_key,
    cast(Validation_Regex__c as string)         as validation_regex,
    cast(Validation_Error_Message__c as string) as validation_error_message,
    cast(Global_Help_Text__c as string)         as global_help_text,
    cast(IsActive__c as boolean)                as is_active,
    cast(Valores_cadastrados__c as numeric)     as valores_cadastrados,
    cast(Form_Section__c as string)             as form_section,
    cast(Custom_LWC_Name__c as string)          as custom_lwc_name,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo')     as _airbyte_extracted_at
from {{ source(source_name, "Service_Attribute_Library__c") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
