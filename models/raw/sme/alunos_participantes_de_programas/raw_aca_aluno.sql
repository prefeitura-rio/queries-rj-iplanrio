
{{ config(
    alias='aca_aluno',
    materialized='table'
) }}

SELECT
    safe_cast(alu_id AS INT64) AS identificador_aluno,
    safe_cast(pes_id AS INT64) AS identificador_pessoa,
    safe_cast(ent_id AS INT64) AS identificador_entidade,
    safe_cast(alu_observacao AS STRING) AS aluno_observacao,
    safe_cast(alu_dataCriacao AS DATETIME) AS aluno_data_criacao_registro,
    safe_cast(alu_dataAlteracao AS DATETIME) AS aluno_data_alteracao_registro,
    safe_cast(alu_situacao AS INT64) AS aluno_situacao,
    safe_cast(alu_dadosIncompletos AS BOOL) AS aluno_dados_incompletos,    
    safe_cast(alu_historicoEscolaIncompleto AS BOOL) as aluno_historico_escolar_incompleto,
    safe_cast(rlg_id AS INT64) AS identificador_religiao,
    safe_cast(alu_meioTransporte AS INT64) AS aluno_meio_transporte,
    safe_cast(alu_tempoDeslocamento AS INT64) AS aluno_tempo_deslocamento,
    safe_cast(alu_regressaSozinho AS BOOL)  AS aluno_regressa_sozinho,  
    safe_cast(alu_dataCadastroFisico AS DATE) AS aluno_data_cadastro_fisico,
    safe_cast(alu_responsavelInfo AS STRING) AS aluno_responsavel_informacao,
    safe_cast(alu_responsavelInfoDoc AS STRING) AS aluno_responsavel_informacao_documento,
    safe_cast(alu_responsavelInfoOrgaoEmissao AS STRING) AS aluno_responsavel_informacao_orgao_emissor,
    safe_cast(alu_aulaReligiao AS BOOL) AS aluno_aula_religiao,
    safe_cast(alu_gemeo AS BOOL) AS aluno_gemeo,
    safe_cast(alu_possuiEmail AS BOOL) AS aluno_possui_email,  
    safe_cast(end_id AS INT64) AS identificador_endereco,
    safe_cast(alu_observacaoSituacao AS STRING) AS aluno_observacao_situacao,
    safe_cast(alu_participaMaisEducacao AS BOOL) AS aluno_participa_mais_educacao,
    safe_cast(alu_participaAlunoPresente AS BOOL) AS aluno_participa_aluno_presente,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'ACA_Aluno') }}
