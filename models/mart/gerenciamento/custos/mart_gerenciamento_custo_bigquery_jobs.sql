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

with base_data as (
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
        CASE
            WHEN end_time IS NOT NULL AND SAFE_CAST(end_time AS STRING) != '' THEN
                extract(date from end_time at time zone 'PST8PDT')
            ELSE NULL
        END as data_faturamento,
        total_bytes_processed / 1024 / 1024 / 1024 / 1024 as tib_processado,
        total_bytes_billed / 1024 / 1024 / 1024 / 1024 as tib_faturado,
        statement_type
    from {{ ref('raw_gcp_bigquery_jobs') }}
    {% if is_incremental() %}
        -- Pega apenas dados novos baseado na data de faturamento
        where data_faturamento > (select max(data_faturamento) from {{ this }})
           or data_faturamento is null
    {% endif %}
),

all_usage_with_multiplier as (
    select
        *,
        case
            statement_type
                when 'SCRIPT' then 0
                when 'CREATE_MODEL' then 50 * 6.25
                else 6.25
        end as multiplicador
    from base_data
),

cost_added as (
    select
        *,
        tib_faturado * multiplicador as custo_estimado_usd,
        tib_faturado as uso_estimado_tib,
        case
            when tib_faturado is not null then true
            else false
        end as job_faturavel  -- indica se foi cobrado (custo > 0)
    from all_usage_with_multiplier
),

-- CTE para taxa de câmbio diária
taxa_diaria as (
    SELECT
        CAST(usage_end_time AS DATE) AS dia,
        AVG(currency_conversion_rate) AS taxa
    FROM
        `dados-rio-billing.billing.gcp_billing_export_*`
    GROUP BY
        dia
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
        custo_estimado_usd * COALESCE(taxa_diaria.taxa, 1) as custo_estimado_reais,
        EXTRACT(YEAR FROM horario_criacao) AS ano,
        REGEXP_EXTRACT(email_usuario, r'^([^@]+@[^.]+)') AS identificador_completo,
        CASE
            WHEN REGEXP_CONTAINS(email_usuario, r'gserviceaccount')
            THEN 'SIM'
            ELSE 'NÃO'
        END AS eh_service_account,
        CASE
            WHEN projeto_id IS NULL THEN 'SND'
            WHEN projeto_id IN (
                'crm-registry',
                'rj-comunicacao-dev',
                'rj-superapp',
                'rj-vision-ai',
                'dados-rio-billing',
                'rj-chatbot',
                'rj-chatbot-dev'
            ) THEN 'IPLANRIO'
            WHEN REGEXP_CONTAINS(projeto_id, r'hackathon') THEN 'IPLANRIO'
            WHEN REGEXP_CONTAINS(projeto_id, r'datario') THEN 'IPLANRIO'
            WHEN REGEXP_CONTAINS(projeto_id, r'^rj-rec') THEN 'RECRIO'
            WHEN REGEXP_CONTAINS(projeto_id, r'^rj-') THEN
                UPPER(REGEXP_EXTRACT(projeto_id, r'^rj-([^-\s]+)'))
            ELSE UPPER(projeto_id)
        END AS orgao_tratado,
        CASE
            WHEN REGEXP_CONTAINS(email_usuario, r'iam\.gserviceaccount\.com$')
                THEN REGEXP_EXTRACT(email_usuario, r'^([^@]+)@iam\.gserviceaccount\.com$')
            ELSE REGEXP_EXTRACT(email_usuario, r'^([^@]+)@')
        END AS identificador_usuario

    from cost_added
    LEFT JOIN taxa_diaria
        ON DATE(cost_added.horario_criacao) = taxa_diaria.dia
)

select *
from final
