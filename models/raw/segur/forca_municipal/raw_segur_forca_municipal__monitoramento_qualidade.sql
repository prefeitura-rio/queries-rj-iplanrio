{{
    config(
        alias="monitoramento_qualidade",
        schema="brutos_forca_municipal",
        materialized="table",
    )
}}

-- Serializa cada linha excluindo colunas de pipeline (id_hash, updated_at, data_particao)
-- e campos derivados (geometry). Usa unnest([src]) para fazer uma única leitura por tabela
-- e derivar a chave de negócio sem data_particao (cross) — a data_particao é mantida
-- separada para permitir também a detecção de duplicatas intra-partição.

with
    linhas as (
        -- tabelas sem geometry
        select
            'ocorrencias_ativas' as tabela,
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            ) as row_key
        from {{ ref('raw_segur_forca_municipal__ocorrencias_ativas') }} src
        union all
        select
            'ocorrencias_historico',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__ocorrencias_historico') }} src
        union all
        select
            'qmd',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd') }} src
        union all
        select
            'qmd_ativos',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_ativos') }} src
        union all
        select
            'qmd_detalhes',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_detalhes') }} src
        union all
        select
            'qmd_plano',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_plano') }} src
        union all
        select
            'qmd_servicos',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_servicos') }} src
        union all
        -- tabelas com geometry (campo derivado — excluído da chave de negócio)
        select
            'ocorrencias_ativas_v2',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__ocorrencias_ativas_v2') }} src
        union all
        select
            'qmd_kml',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_kml') }} src
        union all
        select
            'unidades_ativas',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__unidades_ativas') }} src
        union all
        select
            'unidades_historico',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__unidades_historico') }} src
        union all
        select
            'unit_positions',
            data_particao,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__unit_positions') }} src
    ),

    -- window functions numa única passagem:
    -- cnt_cross: duplicatas entre todas as partições (mesma row_key, qualquer dia)
    -- cnt_intra: duplicatas dentro da mesma partição (bug real de ingestão)
    com_contagem as (
        select
            tabela,
            data_particao,
            row_key,
            count(*) over (partition by tabela, row_key)                   as cnt_cross,
            count(*) over (partition by tabela, data_particao, row_key)    as cnt_intra,
        from linhas
    ),

    por_tabela as (
        select
            tabela,
            count(*)                                                              as total_linhas,
            count(distinct row_key)                                               as registros_unicos,
            -- cross-partition
            countif(cnt_cross > 1)                                                as linhas_dup_cross,
            count(distinct case when cnt_cross > 1 then row_key end)             as grupos_dup_cross,
            -- intra-partition (bug de ingestão)
            countif(cnt_intra > 1)                                                as linhas_dup_intra,
            count(distinct case when cnt_intra > 1
                then concat(cast(data_particao as string), '||', row_key) end)   as grupos_dup_intra,
        from com_contagem
        group by tabela
    ),

    stats_particoes as (
        select
            table_name                                            as tabela,
            count(*)                                              as total_particoes,
            round(avg(total_rows))                                as media_linhas_por_particao,
            min(total_rows)                                       as min_linhas_particao,
            max(total_rows)                                       as max_linhas_particao,
        from `rj-segur.brutos_forca_municipal.INFORMATION_SCHEMA.PARTITIONS`
        where partition_id not in ('__NULL__', '__UNPARTITIONED__')
        group by table_name
    )

select
    -- identificação e frescor
    m.tabela,
    m.ultima_data_particao,
    m.dias_atraso,
    sp.total_particoes,
    m.linhas_ultima_particao,
    round(sp.media_linhas_por_particao)                                        as media_linhas_por_particao,
    sp.min_linhas_particao,
    sp.max_linhas_particao,

    -- volume e unicidade
    m.total_linhas,
    q.registros_unicos,
    round(m.total_linhas / nullif(q.registros_unicos, 0), 2)                   as fator_reextracao,

    -- duplicatas entre partições (esperado em tabelas de snapshot/referência estável)
    q.linhas_dup_cross,
    q.grupos_dup_cross,
    round(100.0 * q.linhas_dup_cross / nullif(m.total_linhas, 0), 2)           as pct_dup_cross,

    -- duplicatas dentro da mesma partição (sinal de bug na ingestão)
    q.linhas_dup_intra,
    q.grupos_dup_intra,
    round(100.0 * q.linhas_dup_intra / nullif(m.total_linhas, 0), 2)           as pct_dup_intra,

    -- pipeline
    m.total_hashes,
    m.ultima_modificacao,
    m.total_megabytes,
from {{ ref('raw_segur_forca_municipal__monitoramento') }} m
left join por_tabela      q  using (tabela)
left join stats_particoes sp using (tabela)
order by m.tabela
