{{
    config(
        alias='ipl_prefixos',
        materialized="table",
        tags=["raw", "ergon", "prefixos"],
        description="Prefixos utilizados pelo IPLANRIO por funcionário, vínculo e mês."
    )
}}

SELECT
    SAFE_CAST(numfunc AS STRING) AS id_funcionario,
    SAFE_CAST(numvinc AS STRING) AS id_vinculo,
    SAFE_CAST(mesano AS DATE) AS mes_ano,
    SAFE_CAST(prefixo AS INT64) AS prefixo,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'ipl_prefixos') }} AS t
