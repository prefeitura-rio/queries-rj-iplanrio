{{
    config(
        schema="brutos_taxirio",
        alias="metricas_motorista_desocupado",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day"
        },
        tags=["raw", "taxirio"],
        description="Medidas de disponibilidade de motoristas"
    )
}}

SELECT
  safe_cast(id as string) as id_metricas_motoriasta_desocupado,
  safe_cast(driver as string) as id_motorista,
  safe_cast(associatedDiscount as string) as id_desconto_associado,
  datetime(timestamp(datetime)) as data_criacao,
  datetime(timestamp(datetime)) as data_criacao_particao

FROM
  {{ source('brutos_taxirio_staging','metricsdriverunoccupieds')}}
