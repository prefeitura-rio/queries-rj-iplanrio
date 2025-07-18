{{
    config(
        schema="brutos_taxirio",
        alias="ranking_corridas",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day"
        },
        tags=["raw", "taxirio"],
        description="Tabela de Ranking de Corridas"
    )
}}  

SELECT
  safe_cast(id as string) as id_ranking_corrida,
  datetime(timestamp(createdAt)) as data_criacao,
  datetime(timestamp(createdAt)) as data_criacao_particao,
  datetime(timestamp(updatedAt)) as data_atualizacao,
  safe_cast(race as string) as id_corrida,  
  to_json_string(competitors) as competidores
 
FROM
  {{ source('brutos_taxirio_staging','rankingraces') }}
