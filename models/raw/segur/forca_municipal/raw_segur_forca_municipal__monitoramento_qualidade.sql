{{
    config(
        alias="monitoramento_qualidade",
        schema="brutos_forca_municipal",
        materialized="table",
    )
}}

-- Serializa cada linha excluindo colunas de pipeline (updated_at, data_particao)
-- e campos derivados (geometry). id_hash é mantido separado para contar runs distintos.
-- Usa unnest([src]) para fazer uma única leitura por tabela.

with
    linhas as (
        -- tabelas sem geometry
        select
            'ocorrencias_historico' as tabela,
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            ) as row_key
        from {{ ref('raw_segur_forca_municipal__ocorrencias_historico') }} src
        union all
        select
            'qmd',
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd') }} src
        union all
        select
            'qmd_detalhes',
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_detalhes') }} src
        union all
        select
            'qmd_plano',
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_plano') }} src
        union all
        select
            'qmd_servicos',
            data_particao,
            id_hash,
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
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__ocorrencias_ativas_v2') }} src
        union all
        select
            'qmd_kml',
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__qmd_kml') }} src
        union all
        select
            'unidades_historico',
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__unidades_historico') }} src
        union all
        select
            'unit_positions',
            data_particao,
            id_hash,
            to_json_string(
                (select as struct * except(id_hash, updated_at, data_particao, geometry)
                 from unnest([src]))
            )
        from {{ ref('raw_segur_forca_municipal__unit_positions') }} src
    ),

    hashes as (
        select tabela, count(distinct id_hash) as total_hashes
        from linhas
        group by tabela
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

    dup as (
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

    particoes as (
        select
            table_name                                                             as tabela,
            partition_id,
            total_rows,
            total_logical_bytes,
            last_modified_time,
            max(partition_id) over (partition by table_name)                      as ultima_partition_id,
        from `rj-segur.brutos_forca_municipal.INFORMATION_SCHEMA.PARTITIONS`
        where partition_id not in ('__NULL__', '__UNPARTITIONED__')
    ),

    frescor as (
        select
            tabela,
            parse_date('%Y%m%d', max(partition_id))                               as ultima_data_particao,
            count(*)                                                              as total_particoes,
            sum(if(partition_id = ultima_partition_id, total_rows, 0))           as linhas_ultima_particao,
            round(avg(total_rows))                                                as media_linhas_por_particao,
            min(total_rows)                                                       as min_linhas_particao,
            max(total_rows)                                                       as max_linhas_particao,
            sum(total_rows)                                                       as total_linhas,
            datetime(max(last_modified_time), 'America/Sao_Paulo')               as ultima_modificacao,
            round(sum(total_logical_bytes) / pow(1024, 2), 4)                    as total_megabytes,
        from particoes
        group by tabela
    )

select
    -- identificação e frescor
    f.tabela,
    f.ultima_data_particao,
    date_diff(current_date('America/Sao_Paulo'), f.ultima_data_particao, day)  as dias_atraso,
    f.total_particoes,
    f.linhas_ultima_particao,
    f.media_linhas_por_particao,
    f.min_linhas_particao,
    f.max_linhas_particao,

    -- volume e unicidade
    f.total_linhas,
    d.registros_unicos,
    round(f.total_linhas / nullif(d.registros_unicos, 0), 2)                   as fator_reextracao,

    -- duplicatas entre partições (esperado em tabelas de snapshot/referência estável)
    d.linhas_dup_cross,
    d.grupos_dup_cross,
    round(100.0 * d.linhas_dup_cross / nullif(f.total_linhas, 0), 2)           as pct_dup_cross,

    -- duplicatas dentro da mesma partição (sinal de bug na ingestão)
    d.linhas_dup_intra,
    d.grupos_dup_intra,
    round(100.0 * d.linhas_dup_intra / nullif(f.total_linhas, 0), 2)           as pct_dup_intra,

    -- pipeline
    h.total_hashes,
    f.ultima_modificacao,
    f.total_megabytes,
from frescor f
join hashes h using (tabela)
left join dup d using (tabela)
order by f.tabela
