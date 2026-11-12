{% macro raw_czrm__work_order_attribute_answer(source_name) %}

select
    cast(Id as string)                           as id,
    cast(IsDeleted as boolean)                   as is_deleted,
    cast(Name as string)                         as name,
    datetime(CreatedDate, 'America/Sao_Paulo')               as created_date,
    cast(CreatedById as string)                  as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')          as last_modified_date,
    cast(LastModifiedById as string)             as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')            as system_modstamp,
    datetime(LastViewedDate, 'America/Sao_Paulo')            as last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo')        as last_referenced_date,
    cast(WorkOrder__c as string)                 as work_order,
    datetime(Answered_At__c, 'America/Sao_Paulo')            as answered_at,
    cast(Attribute_Label__c as string)           as attribute_label,
    cast(Attribute_Type__c as string)            as attribute_type,
    cast(External_Key__c as string)              as external_key,
    cast(Order__c as numeric)                    as attribute_order,
    cast(Section__c as string)                   as section,
    cast(Service_Attribute_Library__c as string) as service_attribute_library,
    cast(Service_Form_Config__c as string)       as service_form_config,
    cast(Value_Boolean__c as boolean)            as value_boolean,
    datetime(Value_DateTime__c, 'America/Sao_Paulo')         as value_date_time,
    cast(Value_Date__c as date)                  as value_date,
    cast(Value_Display__c as string)             as value_display,
    cast(Value_JSON__c as string)                as value_json,
    cast(Value_Long_Text__c as string)           as value_long_text,
    cast(Value_Multipicklist__c as string)       as value_multipicklist,
    cast(Value_Number__c as numeric)             as value_number,
    cast(Value_Text__c as string)                as value_text,
    cast(Is_Internal__c as boolean)              as is_internal,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo')      as _airbyte_extracted_at
from {{ source(source_name, "WorkOrder_Attribute_Answer__c") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
