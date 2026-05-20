{{
    config(
        alias="monitoramento_qualidade",
        schema="brutos_forca_municipal",
        materialized="table",
    )
}}

-- Monitora frescor e volume de todas as tabelas do dataset brutos_forca_municipal.
--
-- Três categorias de materialização, cada uma com indicador de frescor adequado:
-- merge_dedup            → first_seen/last_seen (estado único por id_hash)
-- partitioned_incremental → data_particao (unit_positions: D-1 por design,
-- dias_atraso=1 é normal)
-- table                  → data_particao (full refresh diário)
with
    -- Tabelas merge-dedup: id_hash único, sem partição no destino.
    -- first_seen/last_seen rastreiam a janela do histórico de estados observados.
    -- hashes_duplicados deve ser sempre 0 (garantido pelo merge unique_key).
    merge_dedup as (
        select
            'qmd' as tabela,
            'merge_dedup' as tipo,
            count(*) as total_linhas,
            date(min(first_seen)) as inicio_historico,
            max(last_seen) as ultima_atualizacao,
            count(*) - count(distinct id_hash) as hashes_duplicados
        from {{ ref("raw_segur_forca_municipal__qmd") }}
        union all
        select
            'qmd_plano',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__qmd_plano") }}
        union all
        select
            'qmd_servicos',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__qmd_servicos") }}
        union all
        select
            'qmd_bases',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__qmd_bases") }}
        union all
        select
            'qmd_missoes_geometria',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__qmd_missoes_geometria") }}
        union all
        select
            'qmd_kml_outros',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__qmd_kml_outros") }}
        union all
        select
            'qmd_missoes',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__qmd_missoes") }}
        union all
        select
            'ocorrencias_ativas_v2',
            'merge_dedup',
            count(*),
            date(min(first_seen)),
            max(last_seen),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__ocorrencias_ativas_v2") }}
    ),

    -- unit_positions: insert_overwrite particionado por dia de coleta GPS.
    -- data_particao = D-1 por design — dias_atraso = 1 é comportamento normal.
    -- hashes_duplicados > 0 indica mesma posição GPS repetida na mesma partição.
    unit_pos as (
        select
            'unit_positions' as tabela,
            'partitioned_incremental' as tipo,
            count(*) as total_linhas,
            min(data_particao) as inicio_historico,
            datetime(max(data_particao)) as ultima_atualizacao,
            count(*) - count(distinct id_hash) as hashes_duplicados
        from {{ ref("raw_segur_forca_municipal__unit_positions") }}
    ),

    -- Tabelas table: full refresh diário, sempre substituídas por completo.
    -- hashes_duplicados > 0 indica a API retornou o mesmo registro duas vezes no dump.
    tabelas_full as (
        select
            'ocorrencias_historico' as tabela,
            'table' as tipo,
            count(*) as total_linhas,
            min(data_particao) as inicio_historico,
            datetime(max(data_particao)) as ultima_atualizacao,
            count(*) - count(distinct id_hash) as hashes_duplicados
        from {{ ref("raw_segur_forca_municipal__ocorrencias_historico") }}
        union all
        select
            'unidades_historico',
            'table',
            count(*),
            min(data_particao),
            datetime(max(data_particao)),
            count(*) - count(distinct id_hash)
        from {{ ref("raw_segur_forca_municipal__unidades_historico") }}
    ),

    todos as (
        select *
        from merge_dedup
        union all
        select *
        from unit_pos
        union all
        select *
        from tabelas_full
    ),

    info_schema as (
        select
            table_name as tabela,
            round(sum(total_logical_bytes) / pow(1024, 2), 2) as total_megabytes,
            datetime(
                max(last_modified_time), 'America/Sao_Paulo'
            ) as ultima_modificacao_bq
        from `rj-segur.brutos_forca_municipal.INFORMATION_SCHEMA.PARTITIONS`
        where partition_id is null or partition_id != '__UNPARTITIONED__'
        group by table_name
    )

select
    t.tabela,
    t.tipo as tipo_materializacao,
    t.total_linhas,
    t.inicio_historico,
    t.ultima_atualizacao,
    date_diff(
        current_date('America/Sao_Paulo'), date(t.ultima_atualizacao), day
    ) as dias_atraso,
    t.hashes_duplicados,
    i.total_megabytes,
    i.ultima_modificacao_bq
from todos t
left join info_schema i using (tabela)
order by t.tabela