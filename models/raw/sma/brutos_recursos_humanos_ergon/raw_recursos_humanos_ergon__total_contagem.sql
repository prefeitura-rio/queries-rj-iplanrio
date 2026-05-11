{{
    config(
        alias='total_contagem',
        materialized="table",
        tags=["raw", "ergon", "contagem", "total", "tempo"],
        description="Tabela que armazena o resultado das contagens de tempo para benefícios dos servidores municipais."
    )
}}

SELECT
    safe_cast(numfunc as string) as id_funcionario,
    safe_cast(numvinc as string) as id_vinculo,
    safe_cast(chave as string) AS id_chave,
    SAFE_CAST(finalidade AS STRING) AS finalidade,
    SAFE_CAST(diastot AS int64) AS total_dias,
    SAFE_CAST(diasfpub AS int64) AS diasfpub,
    SAFE_CAST(diasfpubesp AS int64) AS diasfpubesp,
    SAFE_CAST(total_periodos AS STRING) AS total_periodos,
    SAFE_CAST(total_anos AS STRING) AS total_anos,
    SAFE_CAST(data_proximo AS date) AS data_previsao_proximo_periodo,
    SAFE_CAST(nome_proximo AS STRING) AS nome_finalidade_proximo_periodo,
    SAFE_CAST(emp_codigo AS string) AS id_empresa
FROM {{ source('brutos_ergon_staging', 'TOTAL_CONTA') }} AS t