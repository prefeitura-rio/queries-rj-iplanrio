-- Alunos Participantes de Programas - PPE_AlunoProjetoPrograma
-- Tabela que contém informações sobre a participação dos alunos em projetos e programas.   

{{ config(
    alias='ppe_aluno_projeto_programa',
    materialized='table'
) }}

SELECT
    safe_cast(alu_id AS INT64) AS identificador_aluno,
    safe_cast(ppg_id AS INT64) AS identificador_projeto_programa,
    safe_cast(app_id AS INT64) AS identificador_aluno_projeto,
    safe_cast(cal_id AS INT64) AS identificador_calendario_anual,
    safe_cast(esc_id AS INT64) AS identificador_escola,
    safe_cast(uni_id AS INT64) AS identificador_unidade_escolar,
    safe_cast(app_dataEntrada AS DATE) AS data_entrada_programa,
    safe_cast(app_dataSaida AS DATE) AS data_saida_programa,
    case safe_cast(app_situacao AS INT64) 
         when 1 then 'Ativo'
         when 2 then 'Inativo'
         when 3 then 'Excluido'     
         when null then 'Não Informado'
         else 'Não Classificado'
    end as situacao_aluno_projeto,
    safe_cast(app_dataCriacao AS DATETIME) AS data_criacao_registro_base,
    safe_cast(app_dataAlteracao AS DATETIME) AS data_alteracao_registro_base,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'PPE_AlunoProjetoPrograma') }}