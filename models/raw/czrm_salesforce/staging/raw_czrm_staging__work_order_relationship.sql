{{ config(alias="work_order_relationship") }}

select
    cast(Id as string)                      as id,
    cast(OwnerId as string)                 as owner_id,
    cast(IsDeleted as boolean)              as is_deleted,
    cast(Name as string)                    as name,
    cast(CreatedDate as timestamp)          as created_date,
    cast(CreatedById as string)             as created_by_id,
    cast(LastModifiedDate as timestamp)     as last_modified_date,
    cast(LastModifiedById as string)        as last_modified_by_id,
    cast(SystemModstamp as timestamp)       as system_modstamp,
    cast(Case__c as string)                 as `case`,
    cast(Tipo__c as string)                 as tipo,
    cast(WO_Origem__c as string)            as wo_origem,
    cast(WO_Relacionada__c as string)       as wo_relacionada,
    cast(_airbyte_extracted_at as datetime) as _airbyte_extracted_at
from {{ source("brutos_czrm_salesforce_staging", "WorkOrder_Relationship__c") }}
