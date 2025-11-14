{{
    config(
        alias="regiao_planejamento",
        description="Dados de Regiões de Planejamento (RP) do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__regiao_planejamento") }}
