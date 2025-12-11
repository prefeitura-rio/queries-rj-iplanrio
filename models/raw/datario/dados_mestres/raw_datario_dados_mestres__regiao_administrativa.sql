{{
    config(
        alias="regiao_administrativa",
        description="Dados de Regiões Administrativas (RA) do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__regiao_administrativa") }}
