{{
    config(
        alias='aluno_avaliacao_turma',
        schema='brutos_gestao_escolar',
        materialized='incremental',
        incremental_strategy='merge',
        partition_by={
            "field": "data_alteracao",
            "data_type": "timestamp",
            "granularity": "year"
        },
        unique_key=['tur_id', 'alu_id', 'mtu_id', 'aat_id'],
        cluster_by=['alu_id']
    )
}}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_prefect', 'CLS_AlunoAvaliacaoTurma') }}
    {% if is_incremental() %}
      where data_particao in (CAST(current_date AS STRING), CAST(date_sub(current_date, interval 1 day) AS STRING))
    {% endif %}
),
renamed as (
    select
        teste_erro_falha
    from source
), dedup AS (
    select *,
        row_number() over (
            partition by tur_id, alu_id, mtu_id, aat_id
            order by data_alteracao desc
        ) as row_num
    from renamed
)
SELECT *
FROM dedup
WHERE row_num = 1