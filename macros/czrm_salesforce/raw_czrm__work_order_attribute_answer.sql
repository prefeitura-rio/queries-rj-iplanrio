{
    % macro raw_czrm__work_order_attribute_answer (source_name) %
}
SELECT
    cast(Id AS string) AS id,
    cast(IsDeleted AS boolean) AS is_deleted,
    cast(Name AS string) AS name,
    datetime(CreatedDate, 'America/Sao_Paulo') AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo') AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo') AS system_modstamp,
    datetime(LastViewedDate, 'America/Sao_Paulo') AS last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo') AS last_referenced_date,
    cast(WorkOrder__c AS string) AS work_order,
    datetime(Answered_At__c, 'America/Sao_Paulo') AS answered_at,
    cast(Attribute_Label__c AS string) AS attribute_label,
    cast(Attribute_Type__c AS string) AS attribute_type,
    cast(External_Key__c AS string) AS external_key,
    cast(Order__c AS numeric) AS attribute_order,
    cast(Section__c AS string) AS section,
    cast(Service_Attribute_Library__c AS string) AS service_attribute_library,
    cast(Service_Form_Config__c AS string) AS service_form_config,
    cast(Value_Boolean__c AS boolean) AS value_boolean,
    datetime(Value_DateTime__c, 'America/Sao_Paulo') AS value_date_time,
    cast(Value_Date__c AS date) AS value_date,
    cast(Value_Display__c AS string) AS value_display,
    cast(Value_JSON__c AS string) AS value_json,
    cast(Value_Long_Text__c AS string) AS value_long_text,
    cast(Value_Multipicklist__c AS string) AS value_multipicklist,
    cast(Value_Number__c AS numeric) AS value_number,
    cast(Value_Text__c AS string) AS value_text,
    cast(Is_Internal__c AS boolean) AS is_internal,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {
        {
            source (source_name, "WorkOrder_Attribute_Answer__c")
        }
    } {% if is_incremental () %}
WHERE
    SystemModstamp >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}