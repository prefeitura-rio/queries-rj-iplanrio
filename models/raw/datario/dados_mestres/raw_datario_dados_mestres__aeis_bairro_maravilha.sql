{{
    config(
        alias="aeis_bairro_maravilha",
        description="Dados de Áreas de Especial Interesse Social (AEIS) de Bairro Maravilha do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__aeis_bairro_maravilha") }}
