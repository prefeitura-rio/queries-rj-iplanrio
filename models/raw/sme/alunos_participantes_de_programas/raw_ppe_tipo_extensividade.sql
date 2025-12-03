{{ config(
    alias='ppe_tipo_extensividade',
    materialized='table'
) }}

SELECT
    safe_cast(tex_id AS INT64) AS identificador_tipo_extensividade,
    safe_cast(tex_nome AS STRING) AS nome_tipo_extensividade,
    safe_cast(tex_situacao AS INT64) AS situacao_tipo_extensividade,
    safe_cast(tex_dataCriacao AS DATETIME) AS data_criacao_registro_tipo_extensividade,
    safe_cast(tex_dataAlteracao AS DATETIME) AS data_alteracao_registro_tipo_extensividade,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'PPE_TipoExtensividade') }}
