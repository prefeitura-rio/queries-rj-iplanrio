{{
    config(
        alias="qmd_missoes",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd", "tipo_missao"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "qmd_detalhes") }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível na staging
            where
                safe_cast(data_particao as date) = (
                    select max(safe_cast(data_particao as date))
                    from {{ source("brutos_forca_municipal_staging", "qmd_detalhes") }}
                )
        {% endif %}
    ),

    -- uma linha por missão por QMD
    com_missoes as (
        select
            {{ padronize_id("id_hash") }} as id_hash_pai,
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(data_particao as date) as data_particao,
            {{ padronize_id("qmdId") }} as id_qmd,
            lower(trim(qmdstatusativo)) = 'sim' as indicador_ativo,
            missao
        from source, unnest(json_query_array(missoes)) as missao
    ),

    -- uma linha por missão × serviço (unidade alocada)
    com_servicos as (
        select
            id_hash_pai,
            updated_at,
            data_particao,
            id_qmd,
            indicador_ativo,
            missao,
            servico
        from com_missoes, unnest(json_query_array(missao, '$.servicos')) as servico
    ),

    -- campos computados antes do hash para evitar repetição
    renamed as (
        select
            -- id_hash do pai (preservado para rastreabilidade)
            id_hash_pai,
            updated_at,
            data_particao,

            -- identificadores
            id_qmd,
            upper(trim(json_value(servico, '$.nome'))) as id_unidade,

            -- dados
            {{ padronize_id("json_value(missao, '$.missaoId')") }} as id_missao,
            {{ padronize_id("json_value(servico, '$.servicoId')") }} as id_servico,
            upper(json_value(missao, '$.tipo')) as tipo_missao,
            json_value(missao, '$.roteiro') as roteiro,
            safe.parse_time(
                '%H:%M', json_value(missao, '$.horaInicio')
            ) as hora_inicio_missao,
            safe.parse_time(
                '%H:%M', json_value(missao, '$.horaFim')
            ) as hora_fim_missao,
            -- dias: double-encoding resolvido (JSON string → ARRAY<STRING>)
            array(
                select json_value(dia, '$')
                from unnest(json_query_array(json_value(servico, '$.dias'))) as dia
            ) as dias,
            -- execuções planejadas por dia (unnest pertence ao mart)
            json_query(servico, '$.execucoes') as execucoes,
            indicador_ativo,
            upper(
                regexp_extract(
                    nullif(json_value(missao, '$.geometriaWkt'), ''), r'^([A-Z]+)'
                )
            ) as tipo_geometria,

            -- espacial
            nullif(json_value(missao, '$.geometriaWkt'), '') as geometria_wkt,
            safe.st_geogfromtext(
                nullif(json_value(missao, '$.geometriaWkt'), '')
            ) as geometry
        from com_servicos
    ),

    -- id_hash calculado sobre campos da missão (independente do pai)
    com_hash as (
        select
            to_hex(
                md5(
                    to_json_string(
                        struct(
                            id_qmd,
                            id_missao,
                            id_servico,
                            tipo_missao,
                            roteiro,
                            hora_inicio_missao,
                            hora_fim_missao,
                            dias,
                            execucoes,
                            geometria_wkt
                        )
                    )
                )
            ) as id_hash,
            *
        from renamed
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(data_particao) as data_particao,
            max(updated_at) as updated_at,

            -- identificadores
            any_value(id_hash_pai) as id_hash_pai,
            any_value(id_qmd) as id_qmd,
            any_value(id_unidade) as id_unidade,

            -- dados
            any_value(id_missao) as id_missao,
            any_value(id_servico) as id_servico,
            any_value(tipo_missao) as tipo_missao,
            any_value(roteiro) as roteiro,
            any_value(hora_inicio_missao) as hora_inicio_missao,
            any_value(hora_fim_missao) as hora_fim_missao,
            any_value(dias) as dias,
            any_value(execucoes) as execucoes,
            any_value(indicador_ativo) as indicador_ativo,

            -- espacial
            any_value(tipo_geometria) as tipo_geometria,
            any_value(geometria_wkt) as geometria_wkt,
            any_value(geometry) as geometry
        from com_hash
        group by id_hash
    )

select *
from deduplicado