{{
    config(
        alias="aeis",
        description="Dados de Áreas de Especial Interesse Social (AEIS) do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__aeis") }}
