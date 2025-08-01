{{
    config(
        alias="metricas_motorista_desocupado",
        description="Medidas de disponibilidade de motoristas",
    )
}}

select
    safe_cast(id as string) as id_metricas_motoriasta_desocupado,
    safe_cast(driver as string) as id_motorista,
    safe_cast(associateddiscount as string) as id_desconto_associado

from {{ source("brutos_taxirio_staging", "metricsdriverunoccupieds") }}
