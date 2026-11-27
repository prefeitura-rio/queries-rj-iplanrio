{{
    config(
        alias='designacoes_ev',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "designacoes_ev"],
        description="Eventos de designação dos funcionários."
    )
}}

SELECT
    SAFE_CAST(numfunc AS STRING) AS id_funcionario,
    SAFE_CAST(numvinc AS STRING) AS id_vinculo,
    SAFE_CAST(dtini AS DATE) AS data_inicio,
    SAFE_CAST(dtfim AS DATE) AS data_final,
    SAFE_CAST(funcao AS STRING) AS id_funcao,
    SAFE_CAST(setor AS STRING) AS setor,
    SAFE_CAST(obs AS STRING) AS observacoes,
    SAFE_CAST(numfuncsubs1 AS STRING) AS id_funcionario_substituto_1,
    SAFE_CAST(numvincsubs1 AS STRING) AS id_vinculo_substituto_1,
    SAFE_CAST(numfuncsubs2 AS STRING) AS id_funcionario_substituto_2,
    SAFE_CAST(numvincsubs2 AS STRING) AS id_vinculo_substituto_2,
    SAFE_CAST(pontpubl AS INT64) AS pontuacao_publicacao,
    SAFE_CAST(numero_vaga AS STRING) AS numero_vaga,
    SAFE_CAST(pontlei AS INT64) AS pontuacao_lei,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(emp_codigo AS STRING) AS id_empresa,
    SAFE_CAST(id_reg AS STRING) AS id_registro,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'designacoes_ev') }} AS t
