{{ config(
    alias='funcionarios_ativos',
    materialized='table'
    ) }}

SELECT
    CAST(CPF AS string) AS cpf_servidor,
    CAST(NOME AS string) AS nome_servidor,
    CAST(EMAIL AS string) AS email_servidor,
    CAST(MATRICULA_A AS string) AS primeira_matricula_servidor,
    CAST(PREFIXO_A AS INT64) AS prefixo_matricula_a,
    CAST(MATRICULA_B AS string) AS segunda_matricula_servidor,
    CAST(PREFIXO_B AS INT64) AS prefixo_matricula_b,
    CAST(CARGO_A AS string) AS cargo_primeira_matricula_servidor,
    CAST(CARGO_B AS string) AS cargo_segunda_matricula_servidor,
    CAST(DISCIPLINA_A AS string) AS disciplina_primeira_matricula_servidor,
    CAST(DISCIPLINA_B AS string) AS disciplina_segunda_matricula_servidor,
    CAST(JORNADA_A AS string) AS carga_horaria_primeira_matricula_servidor,
    CAST(JORNADA_B AS string) AS carga_horaria_segunda_matricula_servidor,
    CAST(FUNCAO_A AS string) AS funcao_primeira_matricula_servidor,
    CAST(FUNCAO_B AS string) AS funcao_segunda_matricula_servidor,
    CAST(CRE_A AS string) AS cre_primeira_matricula_servidor,
    CAST(CRE_B AS string) AS cre_segunda_matricula_servidor,
    CAST(LOTACAO_A AS string) AS sigla_lotacao_primeira_matricula_servidor,
    CAST(NLOTACAO_A AS string) AS nome_lotacao_primeira_matricula_servidor,
    CAST(LOTACAO_B AS string) AS sigla_lotacao_segunda_matricula_servidor,
    CAST(NLOTACAO_B AS string) AS nome_lotacao_segunda_matricula_servidor
FROM {{ source('brutos_gestao_escolar_rh_staging', 'RHU_FUNCIONARIOSATIVOS') }}
