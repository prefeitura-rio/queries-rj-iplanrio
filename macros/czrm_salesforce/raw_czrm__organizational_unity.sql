{% macro raw_czrm__organizational_unity(source_name) %}

select
    cast(Id as string)                              as id,
    cast(OwnerId as string)                         as owner_id,
    cast(IsDeleted as boolean)                      as is_deleted,
    cast(Name as string)                            as name,
    cast(RecordTypeId as string)                    as record_type_id,
    datetime(CreatedDate, 'America/Sao_Paulo')                    as created_date,
    cast(CreatedById as string)                     as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')               as last_modified_date,
    cast(LastModifiedById as string)                as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')                 as system_modstamp,
    datetime(LastViewedDate, 'America/Sao_Paulo')                 as last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo')             as last_referenced_date,
    cast(External_Integration_Key__c as string)     as external_integration_key,
    cast(Parent_UO__c as string)                    as parent_uo,
    cast(ativo__c as boolean)                       as ativo,
    cast(email_institucional__c as string)          as email_institucional,
    cast(gestor_responsavel__c as string)           as gestor_responsavel,
    cast(natureza_orgao__c as string)               as natureza_orgao,
    cast(permite_atribuir_chamados__c as boolean)   as permite_atribuir_chamados,
    cast(role_id__c as string)                      as role_id,
    cast(queue_id__c as string)                     as queue_id,
    cast(SGRC_UO_Id__c as numeric)                  as sgrc_uo_id,
    cast(telefone_institucional__c as string)       as telefone_institucional,
    cast(Record_Type_Developer_Name__c as string)   as record_type_developer_name,
    cast(WO_update_event_bundle__c as string)       as wo_update_event_bundle,
    cast(main_organizational_unit__c as string)     as main_organizational_unit,
    cast(Unity_type__c as string)                   as unity_type,
    cast(Shortname__c as string)                    as shortname,
    cast(Oversight_Target_UO__c as string)          as oversight_target_uo,
    cast(approval_queue_id__c as string)            as approval_queue_id,
    cast(Integracao_Ativa__c as boolean)            as integracao_ativa,
    cast(SICI_Code__c as string)                    as sici_code,
    cast(content_approval_queue_id__c as string)    as content_approval_queue_id,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo')          as _airbyte_extracted_at
from {{ source(source_name, "Organizational_Unity__c") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
