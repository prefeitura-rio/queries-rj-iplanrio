{{
    config(
        alias='tipo_orgao',
        materialized="table",
        tags=["raw", "ergon", "orgãos", "tipo_orgao"],
        description="Tabela que contém os registros dos tipos de órgãos da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(tipo AS STRING) AS tipo,
    SAFE_CAST(descr AS STRING) AS descricao
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_tipo_orgao') }} AS t