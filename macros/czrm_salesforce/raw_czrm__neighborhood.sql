{% macro raw_czrm__neighborhood(source_name) %}

select
    cast(Id as string)                          as id,
    cast(OwnerId as string)                     as owner_id,
    cast(IsDeleted as boolean)                  as is_deleted,
    cast(Name as string)                        as name,
    datetime(CreatedDate, 'America/Sao_Paulo')                as created_date,
    cast(CreatedById as string)                 as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')           as last_modified_date,
    cast(LastModifiedById as string)            as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')             as system_modstamp,
    datetime(LastViewedDate, 'America/Sao_Paulo')             as last_viewed_date,
    datetime(LastReferencedDate, 'America/Sao_Paulo')         as last_referenced_date,
    cast(codbairro__c as string)                as codbairro,
    cast(codbnum__c as numeric)                 as codbnum,
    cast(RA__c as string)                       as ra,
    cast(AP__c as string)                       as ap,
    cast(RP__c as string)                       as rp,
    cast(Subprefeitura__c as string)            as subprefeitura,
    cast(APS__c as string)                      as aps,
    cast(CRE__c as string)                      as cre,
    cast(Coverage__c as string)                 as coverage,
    cast(Google_Neighborhood_Name__c as string) as google_neighborhood_name,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo')      as _airbyte_extracted_at
from {{ source(source_name, "Neighborhood__c") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
