{{
    config(
        alias="qmd_missoes",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=[
            "last_seen",
            "data_particao",
            "updated_at",
            "id_subarea",
            "id_area",
        ],
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

    -- Versão mais recente de id_subarea e id_area por missão, derivada do base
    -- model qmd_geometria_kml que já fez o join espacial (ST_INTERSECTS + fallback
    -- ST_DISTANCE). Versões históricas da mesma missão (mesmo id_missao + id_qmd,
    -- geometria mudou) são colapsadas pelo QUALIFY — last_seen desc.
    kml_missao as (
        select id_missao, id_qmd, tipo_missao, id_subarea, id_area
        from {{ ref("raw_segur_forca_municipal__qmd_geometria_kml") }}
        where id_missao is not null
        qualify
            row_number() over (partition by id_missao, id_qmd order by last_seen desc)
            = 1
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
            -- dias: ARRAY<STRUCT<week_day, week_day_number>> — consistente com
            -- qmd_servicos.dias
            array(
                select as struct
                    json_value(dia, '$') as week_day,
                    case
                        json_value(dia, '$')
                        when 'seg'
                        then 1
                        when 'ter'
                        then 2
                        when 'qua'
                        then 3
                        when 'qui'
                        then 4
                        when 'sex'
                        then 5
                        when 'sab'
                        then 6
                        when 'dom'
                        then 7
                    end as week_day_number
                from unnest(json_query_array(json_value(servico, '$.dias'))) as dia
            ) as dias,
            -- execuções planejadas: ARRAY<STRUCT<data_hora_inicio, data_hora_fim,
            -- week_day, week_day_number>>
            -- cada entrada representa uma janela temporal de execução da missão
            -- naquele dia.
            -- week_day/week_day_number derivados de data_hora_inicio eliminam a
            -- necessidade
            -- de derivar o dia da semana no mart a partir do GPS.
            -- unnest([scalar]) é usado para calcular dt_i uma única vez e reutilizar
            -- nos 3 campos.
            array(
                select as struct
                    dt_i as data_hora_inicio,
                    datetime(
                        safe_cast(json_value(exec, '$.dataHoraFim') as timestamp),
                        'America/Sao_Paulo'
                    ) as data_hora_fim,
                    case
                        format_date('%u', date(dt_i))
                        when '1'
                        then 'seg'
                        when '2'
                        then 'ter'
                        when '3'
                        then 'qua'
                        when '4'
                        then 'qui'
                        when '5'
                        then 'sex'
                        when '6'
                        then 'sab'
                        when '7'
                        then 'dom'
                    end as week_day,
                    safe_cast(format_date('%u', date(dt_i)) as int64) as week_day_number
                from
                    unnest(
                        json_query_array(json_query(servico, '$.execucoes'))
                    ) as exec,
                    unnest(
                        [
                            datetime(
                                safe_cast(
                                    json_value(exec, '$.dataHoraInicio') as timestamp
                                ),
                                'America/Sao_Paulo'
                            )
                        ]
                    ) as dt_i
            ) as execucoes,
            indicador_ativo,
            upper(
                trim(
                    regexp_extract(
                        nullif(json_value(missao, '$.geometriaWkt'), ''), r'^([A-Z]+)'
                    )
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
            max(updated_at) as updated_at,
            max(data_particao) as data_particao,

            -- identificadores
            any_value(id_hash_pai) as id_hash_pai,
            any_value(id_qmd) as id_qmd,
            any_value(id_unidade) as id_unidade,

            -- dados
            any_value(id_missao) as id_missao,
            any_value(id_servico) as id_servico,
            any_value(tipo_missao) as tipo_missao,
            any_value(roteiro) as roteiro_raw,
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
    ),

    enriquecido as (
        -- roteiro     : roteiro_raw com prefixos operacionais removidos (via macro).
        -- Equivalente ao roteiro de qmd_geometria_kml.
        -- roteiro_norm: normalização de roteiro_raw para derivação de tipo_operacional
        -- (lower + trim + NFD). Calculado sobre raw para preservar os
        -- padrões textuais usados na classificação. Não exposto no final.
        select
            *,
            {{ padronize_roteiro("roteiro_raw") }} as roteiro,
            lower(
                trim(regexp_replace(normalize(roteiro_raw, nfd), r'\pM', ''))
            ) as roteiro_norm
        from deduplicado
    ),

    derivado as (
        select
            * except (roteiro_norm),
            {{ tipo_missao_nome("tipo_missao") }} as tipo_missao_nome,
            {{
                tipo_operacional(
                    tipo_missao="tipo_missao",
                    tipo_geometria="tipo_geometria",
                    geometry="geometry",
                    roteiro_norm="roteiro_norm",
                )
            }} as tipo_operacional,
            {{ slugify("roteiro") }} as id_roteiro
        from enriquecido
    ),

    com_ids_geo as (
        select d.*, kml.id_subarea, kml.id_area
        from derivado d
        left join
            kml_missao kml
            on d.id_missao = kml.id_missao
            and d.id_qmd = kml.id_qmd
            and d.tipo_missao = kml.tipo_missao
    )

select *
from com_ids_geo