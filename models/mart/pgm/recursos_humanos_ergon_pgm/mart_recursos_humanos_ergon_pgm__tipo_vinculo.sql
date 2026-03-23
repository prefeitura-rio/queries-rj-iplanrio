{{
    config(
        alias='tipo_vinculo',
        materialized="table",
        tags=["raw", "ergon", "tipo", "tipo_vinculo"],
        description="Tipos de vínculos funcionais existentes tanto na administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    sigla,
    nome
FROM {{ ref('raw_recursos_humanos_ergon__tipo_vinculo') }} AS t


