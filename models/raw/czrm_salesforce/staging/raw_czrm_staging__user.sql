{{
    config(
        alias="user",
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge"
    )
}}
SELECT
    cast(Id AS string) AS id,
    cast(Username AS string) AS username,
    cast(LastName AS string) AS last_name,
    cast(FirstName AS string) AS first_name,
    cast(Name AS string) AS name,
    cast(CompanyName AS string) AS company_name,
    cast(Division AS string) AS division,
    cast(Department AS string) AS department,
    cast(Title AS string) AS title,
    cast(Street AS string) AS street,
    cast(City AS string) AS city,
    cast(State AS string) AS state,
    cast(PostalCode AS string) AS postal_code,
    cast(Country AS string) AS country,
    cast(Latitude AS numeric) AS latitude,
    cast(Longitude AS numeric) AS longitude,
    cast(GeocodeAccuracy AS string) AS geocode_accuracy,
    cast(Address AS json) AS address,
    cast(Email AS string) AS email,
    cast(EmailPreferencesAutoBcc AS boolean) AS email_preferences_auto_bcc,
    cast(EmailPreferencesAutoBccStayInTouch AS boolean) AS email_preferences_auto_bcc_stay_in_touch,
    cast(EmailPreferencesStayInTouchReminder AS boolean) AS email_preferences_stay_in_touch_reminder,
    cast(SenderEmail AS string) AS sender_email,
    cast(SenderName AS string) AS sender_name,
    cast(Signature AS string) AS signature,
    cast(StayInTouchSubject AS string) AS stay_in_touch_subject,
    cast(StayInTouchSignature AS string) AS stay_in_touch_signature,
    cast(StayInTouchNote AS string) AS stay_in_touch_note,
    cast(Phone AS string) AS phone,
    cast(Fax AS string) AS fax,
    cast(MobilePhone AS string) AS mobile_phone,
    cast(Alias AS string) AS alias,
    cast(CommunityNickname AS string) AS community_nickname,
    cast(BadgeText AS string) AS badge_text,
    cast(IsActive AS boolean) AS is_active,
    cast(TimeZoneSidKey AS string) AS time_zone_sid_key,
    cast(LocaleSidKey AS string) AS locale_sid_key,
    cast(ReceivesInfoEmails AS boolean) AS receives_info_emails,
    cast(ReceivesAdminInfoEmails AS boolean) AS receives_admin_info_emails,
    cast(EmailEncodingKey AS string) AS email_encoding_key,
    cast(UserType AS string) AS user_type,
    cast(StartDay AS string) AS start_day,
    cast(EndDay AS string) AS end_day,
    cast(LanguageLocaleKey AS string) AS language_locale_key,
    cast(EmployeeNumber AS string) AS employee_number,
    cast(DelegatedApproverId AS string) AS delegated_approver_id,
    cast(ManagerId AS string) AS manager_id,
    cast(LastLoginDate AS timestamp) AS last_login_date,
    cast(CreatedDate AS timestamp) AS created_date,
    cast(CreatedById AS string) AS created_by_id,
    cast(LastModifiedDate AS timestamp) AS last_modified_date,
    cast(LastModifiedById AS string) AS last_modified_by_id,
    cast(SystemModstamp AS timestamp) AS system_modstamp,
    cast(PasswordExpirationDate AS timestamp) AS password_expiration_date,
    cast(SuAccessExpirationDate AS date) AS su_access_expiration_date,
    cast(OfflineTrialExpirationDate AS timestamp) AS offline_trial_expiration_date,
    cast(OfflinePdaTrialExpirationDate AS timestamp) AS offline_pda_trial_expiration_date,
    cast(UserPermissionsMarketingUser AS boolean) AS user_permissions_marketing_user,
    cast(UserPermissionsOfflineUser AS boolean) AS user_permissions_offline_user,
    cast(UserPermissionsAvantgoUser AS boolean) AS user_permissions_avantgo_user,
    cast(UserPermissionsCallCenterAutoLogin AS boolean) AS user_permissions_call_center_auto_login,
    cast(UserPermissionsSFContentUser AS boolean) AS user_permissions_sf_content_user,
    cast(UserPermissionsKnowledgeUser AS boolean) AS user_permissions_knowledge_user,
    cast(UserPermissionsInteractionUser AS boolean) AS user_permissions_interaction_user,
    cast(UserPermissionsSupportUser AS boolean) AS user_permissions_support_user,
    cast(ForecastEnabled AS boolean) AS forecast_enabled,
    cast(UserPreferencesActivityRemindersPopup AS boolean) AS user_preferences_activity_reminders_popup,
    cast(
        UserPreferencesEventRemindersCheckboxDefault AS boolean
    ) AS user_preferences_event_reminders_checkbox_default,
    cast(
        UserPreferencesTaskRemindersCheckboxDefault AS boolean
    ) AS user_preferences_task_reminders_checkbox_default,
    cast(UserPreferencesReminderSoundOff AS boolean) AS user_preferences_reminder_sound_off,
    cast(UserPreferencesDisableAllFeedsEmail AS boolean) AS user_preferences_disable_all_feeds_email,
    cast(UserPreferencesDisableFollowersEmail AS boolean) AS user_preferences_disable_followers_email,
    cast(UserPreferencesDisableProfilePostEmail AS boolean) AS user_preferences_disable_profile_post_email,
    cast(
        UserPreferencesDisableChangeCommentEmail AS boolean
    ) AS user_preferences_disable_change_comment_email,
    cast(
        UserPreferencesDisableLaterCommentEmail AS boolean
    ) AS user_preferences_disable_later_comment_email,
    cast(UserPreferencesDisProfPostCommentEmail AS boolean) AS user_preferences_dis_prof_post_comment_email,
    cast(
        UserPreferencesHideCSNGetChatterMobileTask AS boolean
    ) AS user_preferences_hide_csn_get_chatter_mobile_task,
    cast(
        UserPreferencesDisableMentionsPostEmail AS boolean
    ) AS user_preferences_disable_mentions_post_email,
    cast(UserPreferencesDisMentionsCommentEmail AS boolean) AS user_preferences_dis_mentions_comment_email,
    cast(UserPreferencesHideCSNDesktopTask AS boolean) AS user_preferences_hide_csn_desktop_task,
    cast(
        UserPreferencesHideChatterOnboardingSplash AS boolean
    ) AS user_preferences_hide_chatter_onboarding_splash,
    cast(
        UserPreferencesHideSecondChatterOnboardingSplash AS boolean
    ) AS user_preferences_hide_second_chatter_onboarding_splash,
    cast(
        UserPreferencesDisCommentAfterLikeEmail AS boolean
    ) AS user_preferences_dis_comment_after_like_email,
    cast(UserPreferencesDisableLikeEmail AS boolean) AS user_preferences_disable_like_email,
    cast(UserPreferencesSortFeedByComment AS boolean) AS user_preferences_sort_feed_by_comment,
    cast(UserPreferencesDisableMessageEmail AS boolean) AS user_preferences_disable_message_email,
    cast(UserPreferencesDisableBookmarkEmail AS boolean) AS user_preferences_disable_bookmark_email,
    cast(UserPreferencesDisableSharePostEmail AS boolean) AS user_preferences_disable_share_post_email,
    cast(
        UserPreferencesActionLauncherEinsteinGptConsent AS boolean
    ) AS user_preferences_action_launcher_einstein_gpt_consent,
    cast(
        UserPreferencesAssistiveActionsEnabledInActionLauncher AS boolean
    ) AS user_preferences_assistive_actions_enabled_in_action_launcher,
    cast(UserPreferencesEnableAutoSubForFeeds AS boolean) AS user_preferences_enable_auto_sub_for_feeds,
    cast(
        UserPreferencesDisableFileShareNotificationsForApi AS boolean
    ) AS user_preferences_disable_file_share_notifications_for_api,
    cast(
        UserPreferencesShowTitleToExternalUsers AS boolean
    ) AS user_preferences_show_title_to_external_users,
    cast(
        UserPreferencesShowManagerToExternalUsers AS boolean
    ) AS user_preferences_show_manager_to_external_users,
    cast(
        UserPreferencesShowEmailToExternalUsers AS boolean
    ) AS user_preferences_show_email_to_external_users,
    cast(
        UserPreferencesShowWorkPhoneToExternalUsers AS boolean
    ) AS user_preferences_show_work_phone_to_external_users,
    cast(
        UserPreferencesShowMobilePhoneToExternalUsers AS boolean
    ) AS user_preferences_show_mobile_phone_to_external_users,
    cast(UserPreferencesShowFaxToExternalUsers AS boolean) AS user_preferences_show_fax_to_external_users,
    cast(
        UserPreferencesShowStreetAddressToExternalUsers AS boolean
    ) AS user_preferences_show_street_address_to_external_users,
    cast(UserPreferencesShowCityToExternalUsers AS boolean) AS user_preferences_show_city_to_external_users,
    cast(
        UserPreferencesShowStateToExternalUsers AS boolean
    ) AS user_preferences_show_state_to_external_users,
    cast(
        UserPreferencesShowPostalCodeToExternalUsers AS boolean
    ) AS user_preferences_show_postal_code_to_external_users,
    cast(
        UserPreferencesShowCountryToExternalUsers AS boolean
    ) AS user_preferences_show_country_to_external_users,
    cast(
        UserPreferencesShowProfilePicToGuestUsers AS boolean
    ) AS user_preferences_show_profile_pic_to_guest_users,
    cast(UserPreferencesShowTitleToGuestUsers AS boolean) AS user_preferences_show_title_to_guest_users,
    cast(UserPreferencesShowCityToGuestUsers AS boolean) AS user_preferences_show_city_to_guest_users,
    cast(UserPreferencesShowStateToGuestUsers AS boolean) AS user_preferences_show_state_to_guest_users,
    cast(
        UserPreferencesShowPostalCodeToGuestUsers AS boolean
    ) AS user_preferences_show_postal_code_to_guest_users,
    cast(UserPreferencesShowCountryToGuestUsers AS boolean) AS user_preferences_show_country_to_guest_users,
    cast(
        UserPreferencesShowForecastingChangeSignals AS boolean
    ) AS user_preferences_show_forecasting_change_signals,
    cast(
        UserPreferencesLiveAgentMiawSetupDeflection AS boolean
    ) AS user_preferences_live_agent_miaw_setup_deflection,
    cast(UserPreferencesHideS1BrowserUI AS boolean) AS user_preferences_hide_s1_browser_ui,
    cast(UserPreferencesDisableEndorsementEmail AS boolean) AS user_preferences_disable_endorsement_email,
    cast(UserPreferencesPathAssistantCollapsed AS boolean) AS user_preferences_path_assistant_collapsed,
    cast(UserPreferencesCacheDiagnostics AS boolean) AS user_preferences_cache_diagnostics,
    cast(UserPreferencesShowEmailToGuestUsers AS boolean) AS user_preferences_show_email_to_guest_users,
    cast(UserPreferencesShowManagerToGuestUsers AS boolean) AS user_preferences_show_manager_to_guest_users,
    cast(
        UserPreferencesShowWorkPhoneToGuestUsers AS boolean
    ) AS user_preferences_show_work_phone_to_guest_users,
    cast(
        UserPreferencesShowMobilePhoneToGuestUsers AS boolean
    ) AS user_preferences_show_mobile_phone_to_guest_users,
    cast(UserPreferencesShowFaxToGuestUsers AS boolean) AS user_preferences_show_fax_to_guest_users,
    cast(
        UserPreferencesShowStreetAddressToGuestUsers AS boolean
    ) AS user_preferences_show_street_address_to_guest_users,
    cast(
        UserPreferencesLightningExperiencePreferred AS boolean
    ) AS user_preferences_lightning_experience_preferred,
    cast(
        UserPreferencesHideEndUserOnboardingAssistantModal AS boolean
    ) AS user_preferences_hide_end_user_onboarding_assistant_modal,
    cast(
        UserPreferencesHideLightningMigrationModal AS boolean
    ) AS user_preferences_hide_lightning_migration_modal,
    cast(UserPreferencesHideSfxWelcomeMat AS boolean) AS user_preferences_hide_sfx_welcome_mat,
    cast(UserPreferencesHideBiggerPhotoCallout AS boolean) AS user_preferences_hide_bigger_photo_callout,
    cast(UserPreferencesGlobalNavBarWTShown AS boolean) AS user_preferences_global_nav_bar_wt_shown,
    cast(
        UserPreferencesGlobalNavGridMenuWTShown AS boolean
    ) AS user_preferences_global_nav_grid_menu_wt_shown,
    cast(UserPreferencesCreateLEXAppsWTShown AS boolean) AS user_preferences_create_lex_apps_wt_shown,
    cast(UserPreferencesFavoritesWTShown AS boolean) AS user_preferences_favorites_wt_shown,
    cast(
        UserPreferencesRecordHomeSectionCollapseWTShown AS boolean
    ) AS user_preferences_record_home_section_collapse_wt_shown,
    cast(
        UserPreferencesRecordHomeReservedWTShown AS boolean
    ) AS user_preferences_record_home_reserved_wt_shown,
    cast(
        UserPreferencesFavoritesShowTopFavorites AS boolean
    ) AS user_preferences_favorites_show_top_favorites,
    cast(
        UserPreferencesExcludeMailAppAttachments AS boolean
    ) AS user_preferences_exclude_mail_app_attachments,
    cast(
        UserPreferencesSuppressTaskSFXReminders AS boolean
    ) AS user_preferences_suppress_task_sfx_reminders,
    cast(
        UserPreferencesSuppressEventSFXReminders AS boolean
    ) AS user_preferences_suppress_event_sfx_reminders,
    cast(UserPreferencesPreviewCustomTheme AS boolean) AS user_preferences_preview_custom_theme,
    cast(UserPreferencesHasCelebrationBadge AS boolean) AS user_preferences_has_celebration_badge,
    cast(UserPreferencesUserDebugModePref AS boolean) AS user_preferences_user_debug_mode_pref,
    cast(UserPreferencesSRHOverrideActivities AS boolean) AS user_preferences_srh_override_activities,
    cast(
        UserPreferencesReverseOpenActivitiesView AS boolean
    ) AS user_preferences_reverse_open_activities_view,
    cast(UserPreferencesHasSentWarningEmail AS boolean) AS user_preferences_has_sent_warning_email,
    cast(UserPreferencesHasSentWarningEmail238 AS boolean) AS user_preferences_has_sent_warning_email238,
    cast(UserPreferencesHasSentWarningEmail240 AS boolean) AS user_preferences_has_sent_warning_email240,
    cast(
        UserPreferencesBRELookupTableWelcomeMat AS boolean
    ) AS user_preferences_bre_lookup_table_welcome_mat,
    cast(
        UserPreferencesHideBrowseProductRedirectConfirmation AS boolean
    ) AS user_preferences_hide_browse_product_redirect_confirmation,
    cast(
        UserPreferencesHideOnlineSalesAppTabVisibilityRequirementsModal AS boolean
    ) AS user_preferences_hide_online_sales_app_tab_visibility_requirements_modal,
    cast(
        UserPreferencesHideOnlineSalesAppWelcomeMat AS boolean
    ) AS user_preferences_hide_online_sales_app_welcome_mat,
    cast(
        UserPreferencesShowForecastingRoundedAmounts AS boolean
    ) AS user_preferences_show_forecasting_rounded_amounts,
    cast(IsPartner AS boolean) AS is_partner,
    cast(ContactId AS string) AS contact_id,
    cast(AccountId AS string) AS account_id,
    cast(CallCenterId AS string) AS call_center_id,
    cast(Extension AS string) AS extension,
    cast(PortalRole AS string) AS portal_role,
    cast(IsPortalEnabled AS boolean) AS is_portal_enabled,
    cast(FederationIdentifier AS string) AS federation_identifier,
    cast(AboutMe AS string) AS about_me,
    cast(FullPhotoUrl AS string) AS full_photo_url,
    cast(SmallPhotoUrl AS string) AS small_photo_url,
    cast(IsExtIndicatorVisible AS boolean) AS is_ext_indicator_visible,
    cast(OutOfOfficeMessage AS string) AS out_of_office_message,
    cast(MediumPhotoUrl AS string) AS medium_photo_url,
    cast(DigestFrequency AS string) AS digest_frequency,
    cast(DefaultGroupNotificationFrequency AS string) AS default_group_notification_frequency,
    cast(LastViewedDate AS timestamp) AS last_viewed_date,
    cast(LastReferencedDate AS timestamp) AS last_referenced_date,
    cast(BannerPhotoUrl AS string) AS banner_photo_url,
    cast(SmallBannerPhotoUrl AS string) AS small_banner_photo_url,
    cast(MediumBannerPhotoUrl AS string) AS medium_banner_photo_url,
    cast(IsProfilePhotoActive AS boolean) AS is_profile_photo_active,
    cast(IndividualId AS string) AS individual_id,
    cast(Nome_ficticio__c AS string) AS nome_ficticio,
    cast(main_organizational_unit__c AS string) AS main_organizational_unit,
    cast(et4ae5__Default_ET_Page__c AS string) AS et4ae5_default_et_page,
    cast(et4ae5__Default_MID__c AS string) AS et4ae5_default_mid,
    cast(
        et4ae5__ExactTargetForAppExchangeAdmin__c AS boolean
    ) AS et4ae5_exact_target_for_app_exchange_admin,
    cast(
        et4ae5__ExactTargetForAppExchangeUser__c AS boolean
    ) AS et4ae5_exact_target_for_app_exchange_user,
    cast(et4ae5__ExactTargetUsername__c AS string) AS et4ae5_exact_target_username,
    cast(et4ae5__ExactTarget_OAuth_Token__c AS string) AS et4ae5_exact_target_o_auth_token,
    cast(et4ae5__ValidExactTargetAdmin__c AS boolean) AS et4ae5_valid_exact_target_admin,
    cast(et4ae5__ValidExactTargetUser__c AS boolean) AS et4ae5_valid_exact_target_user,
    cast(Is_External_Public_Servant__c AS boolean) AS is_external_public_servant,
    cast(CPF__c AS string) AS cpf,
    cast(Canal_Preferencial__c AS string) AS canal_preferencial,
    cast(_airbyte_extracted_at AS datetime) AS _airbyte_extracted_at
FROM {{source ("brutos_czrm_salesforce_staging", "User")}}
    {% if is_incremental() %}
        where SystemModstamp > (select max(system_modstamp) from {{ this }})
    {% endif %}