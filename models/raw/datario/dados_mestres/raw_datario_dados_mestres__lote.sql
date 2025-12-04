{{
    config(
        alias="lote",
        description="Dados de lotes do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__lote") }}
