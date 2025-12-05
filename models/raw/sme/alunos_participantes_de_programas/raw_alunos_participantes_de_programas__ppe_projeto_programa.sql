-- Alunos Participantes de Programas - PPE_ProjetoPrograma
-- Tabela que contém informações sobre os projetos e programas disponíveis para os alunos.  

{{ config(
    alias='ppe_projeto_programa',
    materialized='table'
) }}

SELECT
    safe_cast(ppg_id AS INT64) AS identificador_projeto_programa,
    safe_cast(ppg_nome AS STRING) AS nome_projeto_programa,
    safe_cast(ppg_expectativaMin AS INT64) AS expectativa_minima_projeto_programa,
    safe_cast(ppg_objetivo AS STRING) AS objetivo_projeto_programa,
    safe_cast(ppg_extensividade AS BOOL) AS extensividade_projeto_programa,
    safe_cast(tex_id AS INT64) AS identificador_tipo_extensividade,
    safe_cast(ppg_situacao AS INT64) AS situacao_projeto_programa,
    safe_cast(ppg_dataCriacao AS DATETIME) AS data_criacao_registro_projeto_programa,
    safe_cast(ppg_dataAlteracao AS DATETIME) AS data_alteracao_registro_projeto_programa,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'PPE_ProjetoPrograma') }}
