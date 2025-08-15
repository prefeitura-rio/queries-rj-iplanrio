{{
    config(
        alias="ranking_corridas",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "date",
            "granularity": "day",
        },
        description="Tabela de Ranking de Corridas",
    )
}}

with
    source as (select * from {{ source("brutos_taxirio_staging", "rankingraces") }}),

    renamed as (
        select
            safe_cast(id as string) as id_ranking_corrida,
            datetime(timestamp(createdat)) as data_criacao,
            cast(timestamp(createdat) as date) as data_criacao_particao,
            datetime(timestamp(updatedat)) as data_atualizacao,
            safe_cast(race as string) as id_corrida,
            to_json_string(competitors) as competidores,
            _airbyte_extracted_at as datalake_loaded_at,    
            current_timestamp() as datalake_transformed_at      
        from source
    )

select *
from renamed
{% if is_incremental() %}
    where data_criacao_particao >= date_add(current_date(), interval -3 day)
{% endif %}
