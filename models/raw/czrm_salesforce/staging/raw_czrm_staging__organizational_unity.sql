{{
    config(
        alias="organizational_unity",
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
    cast(External_Integration_Key__c AS string) AS external_integration_key,
    cast(Parent_UO__c AS string) AS parent_uo,
    cast(ativo__c AS boolean) AS ativo,
    cast(email_institucional__c AS string) AS email_institucional,
    cast(gestor_responsavel__c AS string) AS gestor_responsavel,
    cast(natureza_orgao__c AS string) AS natureza_orgao,
    cast(permite_atribuir_chamados__c AS boolean) AS permite_atribuir_chamados,
    cast(role_id__c AS string) AS role_id,
    cast(queue_id__c AS string) AS queue_id,
    cast(SGRC_UO_Id__c AS numeric) AS sgrc_uo_id,
    cast(telefone_institucional__c AS string) AS telefone_institucional,
    cast(Record_Type_Developer_Name__c AS string) AS record_type_developer_name,
    cast(WO_update_event_bundle__c AS string) AS wo_update_event_bundle,
    cast(main_organizational_unit__c AS string) AS main_organizational_unit,
    cast(Unity_type__c AS string) AS unity_type,
    cast(Shortname__c AS string) AS shortname,
    cast(Oversight_Target_UO__c AS string) AS oversight_target_uo,
    cast(approval_queue_id__c AS string) AS approval_queue_id,
    cast(Integracao_Ativa__c AS boolean) AS integracao_ativa,
    cast(SICI_Code__c AS string) AS sici_code,
    cast(content_approval_queue_id__c AS string) AS content_approval_queue_id,
    cast(_airbyte_extracted_at AS datetime) AS _airbyte_extracted_at
FROM {{source ("brutos_czrm_salesforce_staging","Organizational_Unity__c")}}
    {% if is_incremental() %}
        where SystemModstamp > (select max(system_modstamp) from {{ this }})
    {% endif %}