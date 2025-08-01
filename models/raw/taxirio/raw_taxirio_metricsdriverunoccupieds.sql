{{
    config(
        alias="metricas_motorista_desocupado",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day",
        },
        description="Medidas de disponibilidade de motoristas",
    )
}}

select
    safe_cast(id as string) as id_metricas_motoriasta_desocupado,
    safe_cast(driver as string) as id_motorista,
    safe_cast(associateddiscount as string) as id_desconto_associado,
    datetime(timestamp(datetime)) as data_criacao,
    datetime(timestamp(datetime)) as data_criacao_particao

from {{ source("brutos_taxirio_staging", "metricsdriverunoccupieds") }}
