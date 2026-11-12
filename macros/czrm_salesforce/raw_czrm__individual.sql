{% macro raw_czrm__individual(source_name) %}

select
    cast(Id as string)                              as id,
    cast(OwnerId as string)                         as owner_id,
    cast(IsDeleted as boolean)                      as is_deleted,
    cast(LastName as string)                        as last_name,
    cast(FirstName as string)                       as first_name,
    cast(Salutation as string)                      as salutation,
    cast(Name as string)                            as name,
    cast(HasOptedOutTracking as boolean)            as has_opted_out_tracking,
    cast(HasOptedOutProfiling as boolean)           as has_opted_out_profiling,
    cast(HasOptedOutProcessing as boolean)          as has_opted_out_processing,
    cast(HasOptedOutSolicit as boolean)             as has_opted_out_solicit,
    cast(ShouldForget as boolean)                   as should_forget,
    cast(SendIndividualData as boolean)             as send_individual_data,
    cast(CanStorePiiElsewhere as boolean)           as can_store_pii_elsewhere,
    cast(HasOptedOutGeoTracking as boolean)         as has_opted_out_geo_tracking,
    cast(BirthDate as date)                         as birth_date,
    cast(DeathDate as date)                         as death_date,
    cast(ConvictionsCount as int64)                 as convictions_count,
    cast(ChildrenCount as int64)                    as children_count,
    cast(MilitaryService as string)                 as military_service,
    cast(IsHomeOwner as boolean)                    as is_home_owner,
    cast(Occupation as string)                      as occupation,
    cast(Website as string)                         as website,
    cast(IndividualsAge as string)                  as individuals_age,
    datetime(LastViewedDate, 'America/Sao_Paulo')                 as last_viewed_date,
    cast(MasterRecordId as string)                  as master_record_id,
    cast(ConsumerCreditScore as int64)              as consumer_credit_score,
    cast(ConsumerCreditScoreProviderName as string) as consumer_credit_score_provider_name,
    cast(InfluencerRating as int64)                 as influencer_rating,
    cast(HasPrivacyHold as boolean)                 as has_privacy_hold,
    datetime(CreatedDate, 'America/Sao_Paulo')                    as created_date,
    cast(CreatedById as string)                     as created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo')               as last_modified_date,
    cast(LastModifiedById as string)                as last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo')                 as system_modstamp,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo')          as _airbyte_extracted_at
from {{ source(source_name, "Individual") }}
{% if is_incremental() %}
    where SystemModstamp > (select coalesce(max(system_modstamp), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

{% endmacro %}
