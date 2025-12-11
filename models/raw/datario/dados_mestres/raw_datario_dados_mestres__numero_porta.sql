{{
    config(
        alias="numero_porta",
        description="Dados de números de porta (endereços) do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__numero_porta") }}
