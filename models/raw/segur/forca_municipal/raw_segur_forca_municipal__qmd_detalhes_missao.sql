{{
    config(
        alias="qmd_detalhes_missao",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["id_qmd", "tipo_missao"],
    )
}}

with
    source as (
        select
            id_hash,
            updated_at,
            id_qmd,
            indicador_ativo,
            missoes,
            data_particao
        from {{ ref('raw_segur_forca_municipal__qmd_detalhes') }}
        {% if is_incremental() %}
            where data_particao >= (select max(data_particao) from {{ this }})
        {% endif %}
    ),

    -- uma linha por missão por QMD
    com_missoes as (
        select
            id_hash,
            updated_at,
            id_qmd,
            indicador_ativo,
            missao,
            data_particao
        from source,
            unnest(json_query_array(missoes)) as missao
    ),

    -- uma linha por missão × serviço (unidade alocada)
    com_servicos as (
        select
            id_hash,
            updated_at,
            id_qmd,
            indicador_ativo,
            missao,
            servico,
            data_particao
        from com_missoes,
            unnest(json_query_array(missao, '$.servicos')) as servico
    ),

    renamed as (
        select
            -- metadados da pipeline
            id_hash,
            updated_at,

            -- identificadores (FKs para outras entidades)
            id_qmd,
            upper(trim(json_value(servico, '$.nome'))) as id_unidade,

            -- dados
            {{ padronize_id("json_value(missao, '$.missaoId')")   }} as id_missao,
            {{ padronize_id("json_value(servico, '$.servicoId')") }} as id_servico,
            upper(json_value(missao, '$.tipo'))                       as tipo_missao,
            json_value(missao, '$.roteiro')    as roteiro,
            safe.parse_time(
                '%H:%M', json_value(missao, '$.horaInicio')
            ) as hora_inicio_missao,
            safe.parse_time(
                '%H:%M', json_value(missao, '$.horaFim')
            ) as hora_fim_missao,
            -- dias: double-encoding resolvido (JSON string → ARRAY<STRING>)
            array(
                select json_value(dia, '$')
                from unnest(
                    json_query_array(json_value(servico, '$.dias'))
                ) as dia
            )                                  as dias,
            -- execuções planejadas por dia (unnest pertence ao mart)
            json_query(servico, '$.execucoes') as execucoes,
            indicador_ativo,

            -- espacial
            safe.st_geogfromtext(
                nullif(json_value(missao, '$.geometriaWkt'), '')
            ) as geometry,

            -- partição
            data_particao
        from com_servicos
    )

select *
from renamed
