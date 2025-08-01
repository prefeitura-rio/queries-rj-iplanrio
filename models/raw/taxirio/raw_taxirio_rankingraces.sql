{{
    config(
        schema="brutos_taxirio",
        alias="ranking_corridas",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day",
        },
        tags=["raw", "taxirio"],
        description="Tabela de Ranking de Corridas",
    )
}}

select
    safe_cast(id as string) as id_ranking_corrida,
    datetime(timestamp(createdat)) as data_criacao,
    datetime(timestamp(createdat)) as data_criacao_particao,
    datetime(timestamp(updatedat)) as data_atualizacao,
    safe_cast(race as string) as id_corrida,
    to_json_string(competitors) as competidores

from {{ source("brutos_taxirio_staging", "rankingraces") }}
