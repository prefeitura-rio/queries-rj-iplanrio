{{
    config(
        alias="monitoramento",
        schema="brutos_forca_municipal",
        materialized="table",
    )
}}

with
    hashes as (
        select 'ocorrencias_ativas' as tabela, count(distinct id_hash) as total_hashes
        from {{ ref('raw_segur_forca_municipal__ocorrencias_ativas') }}
        union all
        select 'ocorrencias_ativas_v2', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__ocorrencias_ativas_v2') }}
        union all
        select 'ocorrencias_historico', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__ocorrencias_historico') }}
        union all
        select 'qmd', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__qmd') }}
        union all
        select 'qmd_ativos', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__qmd_ativos') }}
        union all
        select 'qmd_detalhes', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__qmd_detalhes') }}
        union all
        select 'qmd_kml', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__qmd_kml') }}
        union all
        select 'qmd_plano', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__qmd_plano') }}
        union all
        select 'qmd_servicos', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__qmd_servicos') }}
        union all
        select 'unidades_ativas', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__unidades_ativas') }}
        union all
        select 'unidades_historico', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__unidades_historico') }}
        union all
        select 'unit_positions', count(distinct id_hash)
        from {{ ref('raw_segur_forca_municipal__unit_positions') }}
    ),

    particoes as (
        select
            table_name as tabela,
            partition_id,
            total_rows,
            total_logical_bytes,
            last_modified_time,
            max(partition_id) over (partition by table_name) as ultima_partition_id,
        from `rj-segur.brutos_forca_municipal.INFORMATION_SCHEMA.PARTITIONS`
        where partition_id not in ('__NULL__', '__UNPARTITIONED__')
    ),

    por_tabela as (
        select
            tabela,
            parse_date('%Y%m%d', max(partition_id)) as ultima_data_particao,
            sum(if(partition_id = ultima_partition_id, total_rows, 0))
                as linhas_ultima_particao,
            sum(total_rows) as total_linhas,
            max(last_modified_time) as ultima_modificacao,
            round(sum(total_logical_bytes) / pow(1024, 2), 4) as total_megabytes,
        from particoes
        group by tabela
    )

select
    t.tabela,
    t.ultima_data_particao,
    date_diff(
        current_date('America/Sao_Paulo'), t.ultima_data_particao, day
    ) as dias_atraso,
    t.linhas_ultima_particao,
    t.total_linhas,
    h.total_hashes,
    datetime(t.ultima_modificacao, 'America/Sao_Paulo') as ultima_modificacao,
    t.total_megabytes,
from por_tabela t
join hashes h using (tabela)
order by t.tabela
