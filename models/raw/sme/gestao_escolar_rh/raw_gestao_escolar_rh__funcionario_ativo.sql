{{
    config(
        alias='funcionarios_ativos',
        schema='gestao_escolar_rh'
    )
}}

with source as (
    select * FROM {{ source('gestao_escolar_rh_staging', 'RHU_FUNCIONARIOSATIVOS') }}
)

SELECT
    SAFE_CAST(CPF AS STRING) cpf_servidor,
    SAFE_CAST(NOME AS STRING) nome_servidor,
    SAFE_CAST(EMAIL AS STRING) email_servidor,
    SAFE_CAST(MATRICULA_A AS STRING) primeira_matricula_servidor,
    SAFE_CAST(PREFIXO_A AS INT64) prefixo_matricula_a,
    SAFE_CAST(MATRICULA_B AS STRING) segunda_matricula_servidor,
    SAFE_CAST(PREFIXO_B AS INT64) prefixo_matricula_b,
    SAFE_CAST(CARGO_A AS STRING) cargo_primeira_matricula_servidor,
    SAFE_CAST(CARGO_B AS STRING) cargo_segunda_matricula_servidor,
    SAFE_CAST(DISCIPLINA_A AS STRING) disciplina_primeira_matricula_servidor,
    SAFE_CAST(DISCIPLINA_B AS STRING) disciplina_segunda_matricula_servidor,
    SAFE_CAST(JORNADA_A AS STRING) carga_horaria_primeira_matricula_servidor,
    SAFE_CAST(JORNADA_B AS STRING) carga_horaria_segunda_matricula_servidor,
    SAFE_CAST(FUNCAO_A AS STRING) funcao_primeira_matricula_servidor,
    SAFE_CAST(FUNCAO_B AS STRING) funcao_segunda_matricula_servidor,
    SAFE_CAST(CRE_A AS STRING) cre_primeira_matricula_servidor,
    SAFE_CAST(CRE_B AS STRING) cre_segunda_matricula_servidor,
    SAFE_CAST(LOTACAO_A AS STRING) sigla_lotacao_primeira_matricula_servidor,
    SAFE_CAST(NLOTACAO_A AS STRING) nome_lotacao_primeira_matricula_servidor,
    SAFE_CAST(LOTACAO_B AS STRING) sigla_lotacao_segunda_matricula_servidor,
    SAFE_CAST(NLOTACAO_B AS STRING) nome_lotacao_segunda_matricula_servidor
FROM source
