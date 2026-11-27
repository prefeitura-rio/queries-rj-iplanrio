{{
    config(
        alias='averbacoes_conta_gbp',
        materialized="table",
        tags=["raw", "ergon", "averbacoes", "contagem"],
        description="Tabela que armazena o total de dias averbados por tipo de tempo (view GBP)."
    )
}}

SELECT
    SAFE_CAST(m8 AS STRING) AS m8,
    SAFE_CAST(cod_tipo_tempo AS STRING) AS id_tipo_tempo,
    SAFE_CAST(tipo_tempo AS STRING) AS nome_tipo_tempo,
    SAFE_CAST(total_dias AS INT64) AS total_dias,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_averbacoes_conta_gbp') }} AS t
