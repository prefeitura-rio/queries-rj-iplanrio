{% macro raw_czrm__individual (source_name) %}
SELECT
    cast(Id AS string) AS id,
    cast(OwnerId AS string) AS owner_id,
    cast(IsDeleted AS boolean) AS is_deleted,
    cast(LastName AS string) AS last_name,
    cast(FirstName AS string) AS first_name,
    cast(Salutation AS string) AS salutation,
    cast(Name AS string) AS name,
    cast(HasOptedOutTracking AS boolean) AS has_opted_out_tracking,
    cast(HasOptedOutProfiling AS boolean) AS has_opted_out_profiling,
    cast(HasOptedOutProcessing AS boolean) AS has_opted_out_processing,
    cast(HasOptedOutSolicit AS boolean) AS has_opted_out_solicit,
    cast(ShouldForget AS boolean) AS should_forget,
    cast(SendIndividualData AS boolean) AS send_individual_data,
    cast(CanStorePiiElsewhere AS boolean) AS can_store_pii_elsewhere,
    cast(HasOptedOutGeoTracking AS boolean) AS has_opted_out_geo_tracking,
    cast(BirthDate AS date) AS birth_date,
    cast(DeathDate AS date) AS death_date,
    cast(ConvictionsCount AS int64) AS convictions_count,
    cast(ChildrenCount AS int64) AS children_count,
    cast(MilitaryService AS string) AS military_service,
    cast(IsHomeOwner AS boolean) AS is_home_owner,
    cast(Occupation AS string) AS occupation,
    cast(Website AS string) AS website,
    cast(IndividualsAge AS string) AS individuals_age,
    datetime(LastViewedDate, 'America/Sao_Paulo') AS last_viewed_date,
    cast(MasterRecordId AS string) AS master_record_id,
    cast(ConsumerCreditScore AS int64) AS consumer_credit_score,
    cast(ConsumerCreditScoreProviderName AS string) AS consumer_credit_score_provider_name,
    cast(InfluencerRating AS int64) AS influencer_rating,
    cast(HasPrivacyHold AS boolean) AS has_privacy_hold,
    datetime(CreatedDate, 'America/Sao_Paulo') AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    datetime(LastModifiedDate, 'America/Sao_Paulo') AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    datetime(SystemModstamp, 'America/Sao_Paulo') AS system_modstamp,
    datetime(_airbyte_extracted_at, 'America/Sao_Paulo') AS _airbyte_extracted_at
FROM
    {{source (source_name, "Individual")}} {% if is_incremental () %}
WHERE
    SystemModstamp >= (
        SELECT
            coalesce(max(system_modstamp), '1900-01-01 00:00:00')
        FROM
            {{this}}
    ) {% endif %} {% endmacro %}