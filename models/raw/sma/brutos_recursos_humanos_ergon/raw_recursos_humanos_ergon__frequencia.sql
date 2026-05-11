
{{
    config(
        alias='frequencia_ergon',
        materialized="table",
        tags=["raw", "ergon", "frequencia"],
        description="Tabela que armazena as ocorrências de frequência do servidor municipal."
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS string) AS id_funcionario,
    SAFE_CAST(NUMVINC AS string) AS id_vinculo,
    safe_cast(SAFE_CAST(DTINI AS timestamp) as date) AS data_inicio,
    safe_cast(SAFE_CAST(DTFIM AS timestamp) as date) AS data_final,
    SAFE_CAST(TIPOFREQ AS STRING) AS tipo_frequencia,
    SAFE_CAST(CODFREQ AS string) AS id_frequencia,
    SAFE_CAST(OBS AS STRING) AS observacoes,
    SAFE_CAST(EMP_CODIGO AS string) AS id_empresa
FROM {{ source('brutos_ergon_staging', 'FREQUENCIAS') }} AS t
