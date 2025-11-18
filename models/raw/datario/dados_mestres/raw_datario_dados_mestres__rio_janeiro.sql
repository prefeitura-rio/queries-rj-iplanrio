{{
    config(
        alias="rio_janeiro",
        description="Dados do município do Rio de Janeiro"
    )
}}

SELECT * FROM {{ ref("raw_iplanrio_dados_mestres__rio_janeiro") }}
