{{
    config(
        alias="qmd_kml_outros",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

-- Catch-all: captura qualquer kml_folder não reconhecido por qmd_bases ou
-- qmd_missoes_geometria. Em condições normais esta tabela deve ter zero linhas.
-- total_linhas > 0 no monitoramento_qualidade indica que a API introduziu um
-- novo tipo de pasta KML — investigar e criar modelo dedicado.

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd_kml') }}
        where lower(regexp_replace(normalize(kml_folder, NFD), r'\pM', ''))
            not in ('qmds', 'qmd', 'missoes', 'missao')
        {% if is_incremental() %}
            and safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'qmd_kml') }}
            )
        {% endif %}
    ),

    renamed as (
        select
            {{ padronize_id('id_hash') }}               as id_hash,
            safe_cast(updated_at as datetime)           as updated_at,
            safe_cast(data_particao as date)            as data_particao,
            {{ padronize_id('qmd_id') }}                as id_qmd,
            safe_cast(kml_folder as string)             as kml_folder_raw,
            safe_cast(name as string)                   as nome,
            safe_cast(geometry_type as string)          as tipo_geometria,
            safe_cast(description as string)            as descricao,
            safe_cast(extended_data as string)          as dados_extendidos,
            safe_cast(geometry_wkt as string)           as geometria_wkt
        from source
    ),

    deduplicado as (
        select
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(data_particao) as data_particao,
            max(updated_at)    as updated_at,
            any_value(id_qmd)          as id_qmd,
            any_value(kml_folder_raw)  as kml_folder_raw,
            any_value(nome)            as nome,
            any_value(tipo_geometria)  as tipo_geometria,
            any_value(descricao)       as descricao,
            any_value(dados_extendidos) as dados_extendidos,
            any_value(geometria_wkt)   as geometria_wkt
        from renamed
        group by id_hash
    )

select *
from deduplicado
