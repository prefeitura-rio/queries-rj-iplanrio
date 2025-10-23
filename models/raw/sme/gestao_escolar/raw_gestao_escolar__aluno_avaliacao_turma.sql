{{
    config(
        alias='aluno_avaliacao_turma',
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
    select * from {{ source('sme_brutos_gestao_escolar_staging_prefect', 'CLS_AlunoAvaliacaoTurma') }}
    
    {{ incremental_filter() }}

),
renamed as (
    select
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("tur_id") }}), r'\.0$', '') AS STRING) AS tur_id,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("alu_id") }}), r'\.0$', '') AS STRING) AS alu_id,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("mtu_id") }}), r'\.0$', '') AS STRING) AS mtu_id,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_id") }}), r'\.0$', '') AS STRING) AS aat_id,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("fav_id") }}), r'\.0$', '') AS STRING) AS fav_id,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("ava_id") }}), r'\.0$', '') AS STRING) AS ava_id,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_avaliacao") }}), r'\.0$', '') AS STRING) AS aat_avaliacao,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_frequencia") }}), r'\.0$', '') AS STRING) AS aat_frequencia,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_comentarios") }}), r'\.0$', '') AS STRING) AS aat_comentarios,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_relatorio") }}), r'\.0$', '') AS STRING) AS aat_relatorio,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_situacao") }}), r'\.0$', '') AS STRING) AS aat_situacao,
        SAFE_CAST({{ adapter.quote("aat_dataCriacao") }} AS TIMESTAMP) AS data_criacao,
        SAFE_CAST({{ adapter.quote("aat_dataAlteracao") }} AS TIMESTAMP) AS data_alteracao,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_semProfessor") }}), r'\.0$', '') AS STRING) AS aat_semProfessor,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_numeroFaltas") }}), r'\.0$', '') AS INT64) AS aat_numeroFaltas,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_numeroAulas") }}), r'\.0$', '') AS INT64) AS aat_numeroAulas,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("arq_idRelatorio") }}), r'\.0$', '') AS STRING) AS arq_idRelatorio,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_ausenciasCompensadas") }}), r'\.0$', '') AS STRING) AS aat_ausenciasCompensadas,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_avaliacaoAdicional") }}), r'\.0$', '') AS STRING) AS aat_avaliacaoAdicional,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_faltoso") }}), r'\.0$', '') AS STRING) AS aat_faltoso,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_frequenciaAcumulada") }}), r'\.0$', '') AS STRING) AS aat_frequenciaAcumulada,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_registroexterno") }}), r'\.0$', '') AS STRING) AS aat_registroexterno,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_frequenciaAcumuladaCalculada") }}), r'\.0$', '') AS STRING) AS aat_frequenciaAcumuladaCalculada,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_naoAvaliado") }}), r'\.0$', '') AS STRING) AS aat_naoAvaliado,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_avaliacaoPosConselho") }}), r'\.0$', '') AS STRING) AS aat_avaliacaoPosConselho,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_justificativaPosConselho") }}), r'\.0$', '') AS STRING) AS aat_justificativaPosConselho,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("aat_frequenciaFinalAjustada") }}), r'\.0$', '') AS STRING) AS aat_frequenciaFinalAjustada,
        SAFE_CAST({{ adapter.quote("_prefect_extracted_at") }} AS TIMESTAMP) AS loaded_at
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