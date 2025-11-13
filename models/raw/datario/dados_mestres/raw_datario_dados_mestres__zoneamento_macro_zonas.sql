{{
    config(
        alias="zoneamento_macro_zonas",
        description="Dados de Macro Zonas de Zoneamento do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__zoneamento_macro_zonas") }}
