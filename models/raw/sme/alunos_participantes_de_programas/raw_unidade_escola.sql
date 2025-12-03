{{ config(
    alias='unidade_escola',
    materialized='table'
) }}

SELECT
    safe_cast(esc_id AS INT64) AS identificador_escola,
    safe_cast(uni_id AS INT64) AS identificador_unidade_escolar,
    safe_cast(uni_codigo AS STRING) AS codigo_unidade_escolar,
    safe_cast(uni_descricao AS STRING) AS descricao,
    safe_cast(uni_principal AS BOOL) AS principal,
    safe_cast(uni_zona AS INT64) AS zona,
    safe_cast(uni_funcionamentoInicio AS DATE) AS funcionamento_inicio,
    safe_cast(uni_funcionamentoFim AS DATE) AS funcionamento_fim,
    safe_cast(uni_cepsProximos AS STRING) AS ceps_proximos,
    safe_cast(uni_dataCriacao AS DATETIME) AS data_criacao,
    safe_cast(uni_dataAlteracao AS DATETIME) AS data_alteracao,
    safe_cast(uni_situacao AS INT64) AS situacao,
    safe_cast(uni_observacao AS STRING) AS observacao,
    safe_cast(uni_alimentacaoEscolar AS BOOL) AS alimentacao_escolar,
    safe_cast(uni_propostaFormacaoAlternancia AS BOOL) AS proposta_formacao_alternancia,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'ESC_UnidadeEscola') }}
