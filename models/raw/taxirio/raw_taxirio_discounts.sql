{{
    config(
        schema="brutos_taxirio",
        alias="descontos",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["raw", "taxirio"],
        description="Tabela de Descontos",
    )
}}

select
    safe_cast(id as string) as id_desconto_associado,
    safe_cast(description as string) as descricao,
    safe_cast(value as float64) as desconto,
    safe_cast(createdat as date) as data_criacao,
    safe_cast(createdat as date) as data_criacao_particao

from {{ source("brutos_taxirio_staging", "discounts") }}
