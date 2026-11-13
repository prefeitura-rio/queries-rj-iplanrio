{% macro raw_czrm__work_order_update(source_name) %}
SELECT
    cast(Id AS string) AS id,
    cast(IsDeleted AS boolean) AS is_deleted,
    cast(Name AS string) AS name,
    datetime(CreatedDate, 'UTC') AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    datetime(LastModifiedDate, 'UTC') AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    datetime(SystemModstamp, 'UTC') AS system_modstamp,
    datetime(LastViewedDate, 'UTC') AS last_viewed_date,
    datetime(LastReferencedDate, 'UTC') AS last_referenced_date,
    cast(WorkOrder__c AS string) AS work_order,
    cast(Event_Type__c AS string) AS event_type,
    datetime(Response_Deadline__c, 'UTC') AS response_deadline,
    cast(Response_time_days__c AS numeric) AS response_time_days,
    cast(Event_comments__c AS string) AS event_comments,
    cast(Status__c AS string) AS status,
    cast(Allow_Citizen_Visibility__c AS boolean) AS allow_citizen_visibility,
    cast(Is_Visible_to_Citizen__c AS boolean) AS is_visible_to_citizen,
    cast(Approver__c AS string) AS approver,
    cast(Rejection_Reason__c AS string) AS rejection_reason,
    cast(Email_Servidor_Sent__c AS boolean) AS email_servidor_sent,
    cast(Transaction_id__c AS string) AS transaction_id,
    datetime(Approval_Process_Start__c, 'UTC') AS approval_process_start,
    datetime(Approve_Decision_Datetime__c, 'UTC') AS approve_decision_datetime,
    cast(Has_Approval_Process__c AS boolean) AS has_approval_process,
    cast(Justification__c AS string) AS justification,
    cast(Approval_Outcome__c AS string) AS approval_outcome,
    cast(Modification_Reason__c AS string) AS modification_reason,
    cast(Response_To__c AS string) AS response_to,
    datetime(Cancellation_Datetime__c, 'UTC') AS cancellation_datetime,
    cast(Transfer_Target__c AS string) AS transfer_target,
    cast(Modified_comments__c AS string) AS modified_comments,
    cast(Is_Replicated__c AS boolean) AS is_replicated,
    cast(Email_Notification_Sent__c AS boolean) AS email_notification_sent,
    cast(Share_Citizen_Data__c AS boolean) AS share_citizen_data,
    cast(Target_Account__c AS string) AS target_account,
    cast(Target_Group__c AS string) AS target_group,
    cast(Dynamic_Fields_Json__c AS string) AS dynamic_fields_json,
    cast(targetUoId__c AS string) AS target_uo_id,
    cast(Cancellation_Reason__c AS string) AS cancellation_reason,
    datetime(Publication_Datetime__c, 'UTC') AS publication_datetime,
    cast(Origin_Service__c AS string) AS origin_service,
    datetime(Created_date__c, 'UTC') AS data_criacao,
    cast(Notificacao_Proprietario_Sent__c AS boolean) AS notificacao_proprietario_sent,
    cast(WhatsApp_Notification_Sent__c AS boolean) AS whats_app_notification_sent,
    datetime(Data_Hora_Andamento__c, 'UTC') AS data_hora_andamento,
    cast(Integration_Event__c AS string) AS integration_event,
    cast(Status_Especifico_Orgao__c AS string) AS status_especifico_orgao,
    cast(Status_Normalizado__c AS string) AS status_normalizado,
    cast(UO_Informada_Orgao__c AS string) AS uo_informada_orgao,
    cast(Related_WorkOrder__c AS string) AS related_work_order,
    cast(Form_Answers_Json__c AS string) AS form_answers_json,
    datetime(_airbyte_extracted_at, 'UTC') AS _airbyte_extracted_at
FROM
    {{source (source_name, "WorkOrderUpdate__c")}} {% if is_incremental () %}
WHERE
    datetime(SystemModstamp, 'UTC') >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}