-- Custos do BigQuery abertos por execução
-- Baseado na tabela brutos_gcp.gcp_bigquery_jobs
{{
    config(
        schema="gerenciamento_custos",
        alias="custo_bigquery_jobs",
        materialized="incremental",
        unique_key="id_job",
        incremental_strategy="merge",
        partition_by={
            "field": "data_faturamento",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["daily"],
    )
}}

with
    base_data as (
        select
            origem_projeto,
            project_id as projeto_id,
            job_id as id_job,
            user_email as email_usuario,
            job_type as tipo_job,
            query as consulta_sql,
            state as estado_job,
            destination_project_id as projeto_destino_id,
            destination_dataset_id as dataset_destino_id,
            destination_table_id as tabela_destino_id,
            error_result as resultado_erro,
            creation_time as horario_criacao,
            case
                when end_time is not null and safe_cast(end_time as string) != ''
                then extract(date from end_time at time zone 'PST8PDT')
                else null
            end as data_faturamento,
            total_bytes_processed / 1024 / 1024 / 1024 / 1024 as tib_processado,
            total_bytes_billed / 1024 / 1024 / 1024 / 1024 as tib_faturado,
            statement_type
        from {{ ref("raw_gcp_bigquery_jobs") }}
        {% if is_incremental() %}
            -- Pega apenas dados novos baseado na data de faturamento
            where
                data_faturamento > (select max(data_faturamento) from {{ this }})
                or data_faturamento is null
        {% endif %}
    ),

    all_usage_with_multiplier as (
        select
            *,
            case
                statement_type
                when 'SCRIPT'
                then 0
                when 'CREATE_MODEL'
                then 50 * 6.25
                else 6.25
            end as multiplicador
        from base_data
    ),

    cost_added as (
        select
            *,
            tib_faturado * multiplicador as custo_estimado_usd,
            tib_faturado as uso_estimado_tib,
            case when tib_faturado is not null then true else false end as job_faturavel  -- indica se foi cobrado (custo > 0)
        from all_usage_with_multiplier
    ),

    -- CTE para taxa de câmbio diária
    taxa_diaria as (
        select
            cast(usage_end_time as date) as dia, avg(currency_conversion_rate) as taxa
        from `dados-rio-billing.billing.gcp_billing_export_*`
        group by dia
    ),

    final as (
        select
            regexp_extract(projeto_id, r'^rj-([a-z0-9]+)') as orgao,
            origem_projeto,
            projeto_id,
            id_job,
            horario_criacao as datahora_criacao,
            email_usuario,
            tipo_job,
            consulta_sql,
            estado_job,
            struct(
                projeto_destino_id as projeto_id,
                regexp_replace(
                    dataset_destino_id,
                    r'(dev_fantasma__|diego__|miloskimatheus__|pedro__|thiago__|vit__)',
                    ''
                ) as dataset_id,
                tabela_destino_id as tabela_id
            ) as destino,
            tib_processado as tib_processado_quantidade,
            resultado_erro,
            data_faturamento,
            uso_estimado_tib as uso_estimado_tib_quantidade,
            custo_estimado_usd as custo_estimado_valor,
            job_faturavel,

            -- Transformações adicionadas
            custo_estimado_usd * coalesce(taxa_diaria.taxa, 1) as custo_estimado_reais,
            extract(year from horario_criacao) as ano,
            regexp_extract(email_usuario, r'^([^@]+@[^.]+)') as identificador_completo,
            case
                when regexp_contains(email_usuario, r'gserviceaccount')
                then 'SIM'
                else 'NÃO'
            end as eh_service_account,
            case
                when projeto_id is null
                then 'SND'
                when
                    projeto_id in (
                        'crm-registry',
                        'rj-comunicacao-dev',
                        'rj-superapp',
                        'rj-vision-ai',
                        'dados-rio-billing',
                        'rj-chatbot',
                        'rj-chatbot-dev'
                    )
                then 'IPLANRIO'
                when regexp_contains(projeto_id, r'hackathon')
                then 'IPLANRIO'
                when regexp_contains(projeto_id, r'datario')
                then 'IPLANRIO'
                when regexp_contains(projeto_id, r'^rj-rec')
                then 'RECRIO'
                when regexp_contains(projeto_id, r'^rj-')
                then upper(regexp_extract(projeto_id, r'^rj-([^-\s]+)'))
                else upper(projeto_id)
            end as orgao_tratado,
            case
                when regexp_contains(email_usuario, r'iam\.gserviceaccount\.com$')
                then
                    regexp_extract(
                        email_usuario, r'^([^@]+)@iam\.gserviceaccount\.com$'
                    )
                else regexp_extract(email_usuario, r'^([^@]+)@')
            end as identificador_usuario

        from cost_added
        left join taxa_diaria on date(cost_added.horario_criacao) = taxa_diaria.dia
    )

select *
from final
