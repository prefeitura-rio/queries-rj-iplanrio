{{
    config(
        alias="zoneamento_urbano",
        description="Dados de Zoneamento Urbano do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__zoneamento_urbano") }}
