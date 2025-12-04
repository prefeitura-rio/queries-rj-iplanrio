{{
    config(
        alias="bairro",
        description="Dados de bairros do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__bairro") }}
