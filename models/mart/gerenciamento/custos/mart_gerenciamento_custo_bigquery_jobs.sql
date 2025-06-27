-- Custos do BigQuery abertos por execução
-- Ref: https://cloud.google.com/bigquery/docs/information-schema-jobs#compare_on-demand_job_usage_to_billing_data

{{
    config(
        schema="gerenciamento_custos",
        alias="gcp_billing_bigquery_detalhado",
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

{% set first_day_of_month = "date_trunc(current_date('America/Sao_Paulo'), month)" %}

-- JOBS_BY_PROJECT SÓ ARMAZENA DADOS DE 180 DIAS
{% set start_date_query %}
  SELECT COALESCE(DATE_SUB(MAX(data_faturamento), INTERVAL 1 DAY), DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)) AS start_date
  FROM {{ ref('raw_gcp_bigquery_jobs') }}
{% endset %}

{% if is_incremental() %}
  {% set start_date_query %}
    SELECT COALESCE(DATE_SUB(MAX(data_faturamento), INTERVAL 1 DAY), DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)) AS start_date
    FROM {{ this }}
  {% endset %}
  {% if execute %}
    {% set results = run_query(start_date_query) %}
    {% set start_date = results.columns[0].values()[0] %}
    {% set end_date = modules.datetime.datetime.utcnow().strftime('%Y-%m-%d') %}
  {% else %}
    {% set start_date = '2023-01-01' %}  -- valor default para compilação
    {% set end_date = '2099-12-31' %}  -- valor default para compilação
  {% endif %}
{% else %}
  {% set start_date = '2023-01-01' %}
  {% set end_date = '2099-12-31' %}
{% endif %}


-- descoberta de projetos
{% set discover_projects_query %}
  SELECT DISTINCT project_id
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
  WHERE project_id LIKE 'rj-%'
  ORDER BY project_id
{% endset %}

{% if execute %}
  {% set results = run_query(discover_projects_query) %}
  {% set projetos = results.columns[0].values() %}

  {% if projetos | length == 0 %}
    {{ log("ERRO: Nenhum projeto foi descoberto! Verifique as permissões.", info=true) }}
    {{ return("") }}  -- Para a execução se não encontrar projetos
  {% else %}
    {{ log("Descobertos " ~ projetos | length ~ " projetos:", info=true) }}
    {% for projeto in projetos %}
      {{ log("  - " ~ projeto, info=true) }}
    {% endfor %}
  {% endif %}
{% else %}

  {% set projetos = [] %}
{% endif %}


{% set all_queries = [] %}
{% for projeto in projetos %}
    {% set q %}
        select
            '{{ projeto }}' as origem_projeto,
            project_id,
            job_id,
            user_email,
            job_type,
            query,
            state,
            destination_table.project_id as destination_project_id,
            destination_table.dataset_id as destination_dataset_id,
            destination_table.table_id as destination_table_id,
            error_result,
            creation_time,
            end_time,
            statement_type,
            total_bytes_processed,
            total_bytes_billed,
            extract(date from end_time at time zone 'PST8PDT') as data_faturamento
        from `{{ projeto }}`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        where DATE(creation_time) >= DATE('{{ start_date }}')
    {% endset %}
    {% do all_queries.append(q) %}
{% endfor %}

with all_usage as (
    {{ all_queries | join('\nunion all\n') }}
),

-- Deduplicação de jobs com mesmo job_id
deduped_usage as (
    select * except(rn) from (
        select *,
            row_number() over (partition by project_id, job_id order by creation_time desc) as rn
        from all_usage
    ) where rn = 1
),

all_usage_with_multiplier as (
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
        data_faturamento,
        total_bytes_processed / 1024 / 1024 / 1024 / 1024 as tib_processado,
        total_bytes_billed / 1024 / 1024 / 1024 / 1024 as tib_faturado,
        case
            statement_type
                when 'SCRIPT' then 0
                when 'CREATE_MODEL' then 50 * 6.25
                else 6.25
        end as multiplicador
    from deduped_usage
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
            ) as dataset_id, -- ver se inclui o pessoal da iplan dps
            tabela_destino_id as tabela_id
        ) as destino,
        tib_processado as tib_processado_quantidade,
        resultado_erro,
        data_faturamento,
        uso_estimado_tib as uso_estimado_tib_quantidade,
        custo_estimado_usd as custo_estimado_valor,
        job_faturavel
    from cost_added
)

select *
from final
{% if is_incremental() %} where data_faturamento >= {{ first_day_of_month }} {% endif %}
