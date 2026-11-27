{{
    config(
        alias='tipo_vinculo',
        materialized="table",
        tags=["raw", "ergon", "tipo", "tipo_vinculo"],
        description="Tipos de vínculos funcionais existentes tanto na administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_tipo_vinc_') }} AS t