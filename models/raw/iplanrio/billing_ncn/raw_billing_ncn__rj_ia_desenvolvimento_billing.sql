{{
    config(
        alias="billing_rj_ia_desenvolvimento",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- =============================================================================
-- Custo por usuário — rj-ia-desenvolvimento
-- Atribuição proporcional: cost_brl = sku_cost × (user_calls / model_total_calls)
--
-- Fontes:
-- src_billing     : {{ ref('raw_billing_ncn__billing_gcp') }}
-- src_all_logs    : {{ source('rj_ia_desenvolvimento_analyticslink', '_AllLogs') }}
-- src_data_access : {{ source('rj_ia_desenvolvimento_auditlog', 'cloudaudit_googleapis_com_data_access') }}
-- =============================================================================
with

    -- =============================================================================
    -- SOURCE — importação pura, sem transformação
    -- =============================================================================
    src_billing as (
        select usage_date, cost, total_credit, service_description
        from {{ ref("raw_billing_ncn__billing_gcp") }}
        where
            project_id = 'rj-ia-desenvolvimento'
            {% if is_incremental() %}
                and usage_date >= (select max(date) from {{ this }})
            {% endif %}
    ),

    src_all_logs as (
        select
            timestamp,
            proto_payload.audit_log.resource_name as resource_name,
            proto_payload.audit_log.authentication_info.principal_email
            as principal_email
        from {{ source("rj_ia_desenvolvimento_analyticslink", "_AllLogs") }}
        where
            proto_payload.audit_log.service_name = 'aiplatform.googleapis.com'
            and proto_payload.audit_log.authentication_info.principal_email is not null
            and (operation.last = true or operation.last is null)
            {% if is_incremental() %}
                and date(timestamp) >= (select max(date) from {{ this }})
            {% endif %}
    ),

    src_data_access as (
        select
            timestamp,
            protopayload_auditlog.resourcename as resource_name,
            protopayload_auditlog.authenticationinfo.principalemail as principal_email
        from
            {{
                source(
                    "rj_ia_desenvolvimento_auditlog",
                    "cloudaudit_googleapis_com_data_access",
                )
            }}
        where
            protopayload_auditlog.servicename = 'aiplatform.googleapis.com'
            and protopayload_auditlog.authenticationinfo.principalemail is not null
            and (operation.last = true or operation.last is null)
            {% if is_incremental() %}
                and date(timestamp) >= (select max(date) from {{ this }})
            {% endif %}
    ),

    -- =============================================================================
    -- STAGING — normalização e combinação
    -- =============================================================================
    billing_classified as (
        select
            usage_date as date,
            coalesce(cost, 0) + coalesce(total_credit, 0) as cost,
            service_description,

            -- Chave normalizada para join com auditlog
            case
                when service_description = 'Opus 4.6'
                then 'claude-opus-4-6'
                else lower(replace(replace(service_description, ' ', '-'), '.', '-'))
            end as model_key

        from src_billing
    ),

    auditlog_combined as (
        -- Combina as duas fontes sem overlap:
        -- src_all_logs    → registros ANTES do início da src_data_access (7 dias)
        -- src_data_access → todos os seus registros (retenção longa, cresce a partir
        -- de 28/05)
        select * except (resource_name)
        from
            (
                select
                    date(timestamp) as date,
                    resource_name,
                    regexp_extract(resource_name, r'/models/([^@/]+)') as model_name,
                    principal_email as user
                from
                    (
                        select timestamp, resource_name, principal_email
                        from src_all_logs
                        where timestamp < (select min(timestamp) from src_data_access)

                        union all

                        select timestamp, resource_name, principal_email
                        from src_data_access
                    )
            )
        where
            model_name is not null
            and model_name != 'count-tokens'
            and date <= (select max(date) from billing_classified)
    ),

    -- =============================================================================
    -- PROPORÇÕES — atribuição de custo por usuário
    -- =============================================================================
    user_calls as (
        select date, model_name, user, count(*) as calls
        from auditlog_combined
        group by date, model_name, user
    ),

    model_calls as (
        select date, model_name, sum(calls) as total_calls from user_calls group by 1, 2
    ),

    user_proportions as (
        select
            u.date,
            u.model_name,
            u.user,
            u.calls,
            safe_divide(u.calls, m.total_calls) as call_fraction
        from user_calls u
        join model_calls m on u.date = m.date and u.model_name = m.model_name
    ),

    -- =============================================================================
    -- OUTPUT
    -- =============================================================================
    final as (
        select
            b.date,
            b.service_description,
            coalesce(u.user, 'no_audit_data_access') as service_account,
            regexp_replace(
                regexp_extract(coalesce(u.user, 'no_audit_data_access'), r'^([^@]+)'),
                r'^claude-',
                ''
            ) as user,
            round(sum(b.cost * coalesce(u.call_fraction, 1)), 4) as cost_brl,
            max(u.calls) as model_calls

        from billing_classified b
        left join user_proportions u on b.date = u.date and b.model_key = u.model_name

        group by 1, 2, 3, 4
    )

select date, service_description, service_account, user, cost_brl, model_calls
from final