{{
    config(
        alias="logradouro",
        description="Dados de logradouros (ruas) do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__logradouro") }}
