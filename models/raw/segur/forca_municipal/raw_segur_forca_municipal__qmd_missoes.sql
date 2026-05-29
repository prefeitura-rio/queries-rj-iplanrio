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
                    case
                        json_value(dia, '$')
                        when 'seg' then 'Segunda'
                        when 'ter' then 'Terça'
                        when 'qua' then 'Quarta'
                        when 'qui' then 'Quinta'
                        when 'sex' then 'Sexta'
                        when 'sab' then 'Sábado'
                        when 'dom' then 'Domingo'
                    end as week_day,
                    case
                        json_value(dia, '$')
                        when 'seg' then 2
                        when 'ter' then 3
                        when 'qua' then 4
                        when 'qui' then 5
                        when 'sex' then 6
                        when 'sab' then 7
                        when 'dom' then 1
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
                        extract(dayofweek from date(dt_i))
                        when 1 then 'Domingo'
                        when 2 then 'Segunda'
                        when 3 then 'Terça'
                        when 4 then 'Quarta'
                        when 5 then 'Quinta'
                        when 6 then 'Sexta'
                        when 7 then 'Sábado'
                    end as week_day,
                    extract(dayofweek from date(dt_i)) as week_day_number
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

    -- -------------------------------------------------------------------------
    -- Polígonos de referência derivados do próprio qmd_missoes.
    -- RF/SV/SP com tipo_operacional='subarea' (≤10 km²): zonas operacionais.
    -- RF/SV/SP com tipo_operacional='area'   (>10 km²): bases inteiras.
    -- Chave de deduplicação: (id_qmd, id_servico) — plano semanal da unidade.
    -- Confirmado nos dados: sempre exatamente 1 RF subarea por (id_qmd, id_servico).
    -- -------------------------------------------------------------------------
    -- id_subarea: resolvido por equi-join (id_qmd, id_servico).
    -- id_servico é o plano semanal da unidade — cobre todas as suas missões
    -- na semana (DS + PTR/PB + RF). Confirmado nos dados:
    -- · 100% dos PTR e PB têm exatamente 1 RF subarea no mesmo id_servico.
    -- · Join é 1:1 por construção — sem ST_INTERSECTS, sem QUALIFY no join.
    -- QUALIFY de segurança: preferência RF sobre SV/SP no caso raro de coexistência.
    subareas as (
        select id_qmd, id_servico, id_roteiro as id_subarea
        from derivado
        where tipo_operacional = 'subarea'
        qualify
            row_number() over (
                partition by id_qmd, id_servico
                order by case tipo_missao when 'RF' then 1 else 2 end
            )
            = 1
    ),

    -- id_area: mesmo padrão de id_subarea — equi-join primário (id_qmd, id_servico),
    -- fallback ST_DISTANCE cross-QMD para o que não resolver.
    areas as (
        select id_qmd, id_servico, geometry, id_roteiro as id_area
        from derivado
        where tipo_operacional = 'area'
        qualify
            row_number() over (
                partition by id_qmd, id_servico
                order by case tipo_missao when 'RF' then 1 else 2 end
            ) = 1
    ),

    -- -------------------------------------------------------------------------
    -- id_subarea: equi-join por (id_qmd, id_servico) — sem custo espacial.
    -- · subarea: identifica-se com sua própria zona (id_roteiro).
    -- · patrulha/posto: join direto ao RF do mesmo plano semanal.
    -- · DS/area/NULL: id_subarea = NULL.
    -- -------------------------------------------------------------------------
    com_ids_subarea as (
        select
            d.*,
            coalesce(
                case when d.tipo_operacional = 'subarea' then d.id_roteiro end,
                s.id_subarea
            ) as id_subarea
        from derivado d
        left join
            subareas s
            on d.id_qmd = s.id_qmd
            and d.id_servico = s.id_servico
            and d.tipo_operacional in ('patrulha', 'posto')
    ),

    -- -------------------------------------------------------------------------
    -- id_area: equi-join primário (id_qmd, id_servico) + fallback ST_DISTANCE.
    -- · area: identifica-se com sua própria zona (id_roteiro).
    -- · patrulha/posto/subarea: equi-join resolve se area estiver no mesmo plano;
    -- fallback ST_DISTANCE cross-QMD cobre o caso normal (QMDs distintos).
    -- · DS/NULL: id_area = NULL.
    -- -------------------------------------------------------------------------
    com_ids_area as (
        select
            cs.*,
            coalesce(
                case when cs.tipo_operacional = 'area' then cs.id_roteiro end,
                a_serv.id_area,
                a_geo.id_area
            ) as id_area
        from com_ids_subarea cs
        left join
            areas a_serv
            on cs.id_qmd = a_serv.id_qmd
            and cs.id_servico = a_serv.id_servico
            and cs.tipo_operacional in ('patrulha', 'posto', 'subarea')
        left join areas a_geo on cs.tipo_operacional in ('patrulha', 'posto', 'subarea')
        qualify
            row_number() over (
                partition by cs.id_hash
                order by
                    (a_serv.id_area is not null) desc,
                    st_distance(cs.geometry, a_geo.geometry) nulls last,
                    coalesce(a_serv.id_area, a_geo.id_area) nulls last
            )
            = 1
    )

select *
from com_ids_area