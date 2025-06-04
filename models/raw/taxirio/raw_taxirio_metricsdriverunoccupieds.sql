{{
    config(
        schema="brutos_taxirio",
        alias="metricas_motorista_desocupado",
        materialized="table",
        partition_by={
            "field": "data_criacao",
            "data_type": "timestamp",
            "granularity": "day"
        },
        tags=["raw", "taxirio", "metricas_motorista_desocupado"],
        description="Medidas de disponibilidade de motoristas"
    )
}}

SELECT
  SAFE_CAST (id as STRING) as id_metricas_motoriasta_desocupado,
  SAFE_CAST (driver as STRING) as id_motorista,
  SAFE_CAST (associatedDiscount as STRING) as id_desconto_associado,
  DATETIME (TIMESTAMP(datetime)) as data_criacao,
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,

FROM
  {{ source('brutos_taxirio_staging','metricsdriverunoccupieds')}}
