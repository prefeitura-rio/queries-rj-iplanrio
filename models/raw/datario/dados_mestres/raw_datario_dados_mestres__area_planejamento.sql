{{
    config(
        alias="area_planejamento",
        description="Dados de Áreas de Planejamento (AP) do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__area_planejamento") }}
