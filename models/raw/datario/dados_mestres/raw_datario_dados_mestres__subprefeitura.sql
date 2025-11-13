{{
    config(
        alias="subprefeitura",
        description="Dados de subprefeituras do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__subprefeitura") }}
