{{
    config(
        schema="gerenciamento_custos",
        alias="jobs_historico",
        materialized="incremental",
        unique_key=["project_id", "job_id"],
        partition_by={
            "field": "data_faturamento",
            "data_type": "date",
            "granularity": "month"
        },
        tags=["historico_jobs", "incremental"]
    )
}}

-- Definição do período incremental
{% if is_incremental() and execute %}
  {% set start_date_query %}
    SELECT COALESCE(DATE_SUB(MAX(data_faturamento), INTERVAL 1 DAY), DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)) AS start_date
    FROM {{ this }}
  {% endset %}
  {% if execute %}
    {% set results = run_query(start_date_query) %}
    {% set start_date = results.columns[0].values()[0] %}
  {% else %}
    {% set start_date = '2023-01-01' %}  -- valor default para compilação
  {% endif %}
{% else %}
  {% set start_date = (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(days=180)).strftime('%Y-%m-%d') %}
{% endif %}

-- Descoberta de projetos
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
    {{ return("") }}
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
        where DATE(creation_time) > DATE('{{ start_date }}')
    {% endset %}
    {% do all_queries.append(q) %}
{% endfor %}

with all_usage as (
    {{ all_queries | join('\nunion all\n') }}
),

deduped_usage as (
    select * except(rn) from (
        select *,
            row_number() over (partition by project_id, job_id order by creation_time desc) as rn
        from all_usage
    ) where rn = 1
)

select
    origem_projeto,
    project_id,
    job_id,
    user_email,
    job_type,
    query,
    state,
    destination_project_id,
    destination_dataset_id,
    destination_table_id,
    error_result,
    creation_time,
    end_time,
    statement_type,
    total_bytes_processed,
    total_bytes_billed,
    data_faturamento
from deduped_usage
